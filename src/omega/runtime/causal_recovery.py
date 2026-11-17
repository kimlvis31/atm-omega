"""Stable inference host with one cooperative recovery-journal owner.

Strict no-obligation installation and historical carry share original resource
checks. Cold startup requires explicit audited new-owner adoption.
"""

import queue
import threading
from copy import deepcopy
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from uuid import uuid4

from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.domain.inference import checksum, decoded, digest, encoded, identity
from omega.runtime.causal_actor_retirement import validate_retirement_value
from omega.runtime.causal_capacity import EvidenceBudgetLimits, evidence_storage_unavailable
from omega.runtime.causal_cold_adoption import (
    classify_interrupted_cold_adoption,
    validate_original_cold_observations,
)
from omega.runtime.causal_generation_facade import GenerationBinding, GenerationFeatureProvider
from omega.runtime.causal_generation_outcomes import (
    FeatureSettlementSnapshot,
    GenerationContext,
    GenerationSettlementSnapshot,
)
from omega.runtime.causal_historical_carry import (
    CarryInstallRequest,
    CarryManifest,
    build_carry_manifest,
    build_cold_carry_manifest,
    read_default_carry_heads,
    verify_clean_closed_carry_manifest,
)
from omega.runtime.causal_inference_registry import FLAGS, operation_id, parse_request
from omega.runtime.causal_inference_service import CausalInferenceService
from omega.runtime.causal_nn_proof import wall_ms
from omega.runtime.causal_recovery_proofs import (
    FaultPauseBarrierAck,
    RecoveryProofArchive,
    RestorationReceipt,
    exact_ack,
    original_observation_document,
    pending_unknown_evidence,
    retired_generation_carry_proof,
    retired_generation_proof,
    startup_certificate_document,
    verified_journal_head,
    verify_carry_acceptance,
)
from omega.runtime.causal_recovery_records import (
    FeatureMutationRef,
    FeatureMutationRejected,
    FeatureMutationUnknown,
    RecoveryJournal,
)
from omega.runtime.causal_root_identity import capture_managed_root_identity
from omega.runtime.causal_startup_policy import (
    ManualStartupCertificate,
    manual_startup_policy_sha256,
)
from omega.runtime.control import InstanceLock

RECOVERY_PARAMETERS = frozenset(
    {
        "core_epoch",
        "expected_generation",
        "expected_control_revision",
        "expected_actor_token",
        "expected_retirement_sha256",
        "state_policy",
        "confirm",
    }
)


def validate_recovery_parameters(parameters):
    if type(parameters) is not dict or set(parameters) != RECOVERY_PARAMETERS:
        raise ValidationError("exact causal recovery parameters required")
    for key in ("core_epoch", "expected_actor_token"):
        identity(parameters[key])
    for key in ("expected_generation", "expected_control_revision"):
        if type(parameters[key]) is not int or not 0 <= parameters[key] < 2**63:
            raise ValidationError("bounded causal recovery revision required")
    if parameters["expected_retirement_sha256"] is not None:
        checksum(parameters["expected_retirement_sha256"])
    if parameters["state_policy"] != "reset" or parameters["confirm"] is not True:
        raise ValidationError("explicit confirmed reset recovery required")
    return digest(parameters)


@dataclass(slots=True)
class _Ticket:
    kind: str
    arguments: tuple
    done: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)
    result: object = None
    error: BaseException | None = None
    abandoned: bool = False


@dataclass(slots=True, eq=False)
class _ActorBinding:
    context: GenerationContext
    actor: object
    facade_binding: GenerationBinding


class CausalInferenceHost:
    """Keep the external provider identity stable across explicit replacement.

    Only recovery admission is asynchronous. Existing normal control submission
    retains the concrete actor's durable admission response.
    """

    def __init__(
        self,
        root,
        *,
        core_epoch,
        training_job_provider,
        on_incident=None,
        evidence_limits=None,
        on_storage_warning=None,
        actor_factory=CausalInferenceService,
        journal_factory=RecoveryJournal,
        clock_ms=None,
        ack_timeout_s=5,
        close_timeout_s=30,
        core_instance=None,
    ):
        self.root, self.core_epoch = Path(root).absolute(), identity(core_epoch)
        if (
            not callable(training_job_provider)
            or not callable(actor_factory)
            or not callable(journal_factory)
            or not 0 < ack_timeout_s <= 30
            or not 0 < close_timeout_s <= 60
        ):
            raise ValidationError("bounded host ownership and timeouts required")
        self.evidence_limits = evidence_limits or EvidenceBudgetLimits()
        if type(self.evidence_limits) is not EvidenceBudgetLimits:
            raise ValidationError("parsed evidence limits required")
        self.training_job_provider, self.on_incident = training_job_provider, on_incident
        self.on_storage_warning = on_storage_warning
        self.clock_ms = clock_ms or wall_ms
        self.ack_timeout_s, self.close_timeout_s = ack_timeout_s, close_timeout_s
        self._actor_factory, self._journal_factory = actor_factory, journal_factory
        self._host_token = uuid4().hex
        if core_instance is not None and type(core_instance) is not InstanceLock:
            raise ValidationError("exact current Core root lease required")
        self._core_instance = core_instance
        self._core_stream = None if core_instance is None else core_instance.stream
        self._managed_root_identity_sha256 = capture_managed_root_identity(
            self.root, core_instance, original_stream=self._core_stream
        )
        self._lock, self._queue = threading.Lock(), queue.Queue(maxsize=256)
        self._started, self._finished = threading.Event(), threading.Event()
        self._journal = self._journal_scope = self._journal_retirement = None
        self._proof_archive = None
        self._current = None
        self._factory_attempted = 0
        self._history, self._retained_candidates = [], []
        self._close_errors = []
        self._terminal_coverage = {}
        self._completed_terminal = {}
        self._consumed_terminal, self._not_dispatched = {}, {}
        self._cold_settlement_blocked = False
        self._carry_available = False
        self._carry_manifest_sha256 = None
        self._last_carry_counts = {"pins": 0, "reservations": 0, "unknown_reservations": 0}
        self._operations, self._controls = {}, {}
        self._recovery = None
        self._adoption_cache = {
            "schema_version": 1,
            "scope": "cold_startup_observation",
            "core_epoch": self.core_epoch,
            "eligible": False,
            "observation_sha256": None,
            "blockers": ["NOT_CLASSIFIED"],
        }
        self._cold_prior_document = None
        self._host_document, self._host_ack = {}, None
        self._startup_error = None
        self._journal_unknown = False
        self._current_journal_write_attempted = False
        self._shutdown_requested = False
        self._preserve_shutdown_requested = self._namespace_preserved = False
        self._core_shutdown_token = None
        self._never_started_closed = False
        self.thread, self.closed, self.closing = None, False, False
        self.external_features = GenerationFeatureProvider(self, self.clock_ms)
        self._recovery_cache = self._summary("STARTING", ())
        self._empty_cache = {
            **FLAGS,
            "schema_version": 1,
            "core_epoch": self.core_epoch,
            "revision": 0,
            "state": "disabled",
            "ready": False,
            "storage_available": True,
            "evidence_storage": evidence_storage_unavailable(self.evidence_limits),
            "actor_alive": False,
            "server_time_ms": 0,
            "active_deployment_id": None,
            "deployments": [],
            "streams": [],
            "operations": [],
            "error": None,
            "limits": {
                "active_deployments": 1,
                "streams": 128,
                "worker_queue": 8,
                "control_queue": 32,
                "operation_history": 256,
                "deployment_history": 32,
            },
        }

    def _summary(self, state, blockers, *, operation=None, retirement_sha256=None):
        status = self.external_features.status() if hasattr(self, "external_features") else None
        return {
            "schema_version": 1,
            "core_epoch": self.core_epoch,
            "state": state,
            "generation": None if status is None else status.generation,
            "control_revision": 0 if status is None else status.control_revision,
            "actor_token": None if status is None else status.actor_token,
            "held": True if status is None else status.held,
            "current_recovery_id": operation,
            "blockers": sorted(
                set(blockers)
                | ({"CORE_SHUTDOWN_REQUESTED"} if self._core_shutdown_token else set())
            ),
            "core_shutdown_operation_id": self._core_shutdown_token,
            "retirement_sha256": retirement_sha256,
            "scope": (
                "verified_historical_pin_carry"
                if self._carry_available
                else "verified_pins0_reservations0"
            ),
            "historical_carry_available": self._carry_available,
            "carry_manifest_sha256": self._carry_manifest_sha256,
            "last_carry_counts": dict(self._last_carry_counts),
            "cold_interrupted_adoption_available": bool(self._adoption_cache.get("eligible")),
            "automatic_inference_started": False,
            "financial_resumed": False,
        }

    def _publish_recovery(
        self, state, blockers=(), *, operation=None, retirement_sha256=None, expected_binding=None
    ):
        result = self._summary(
            state, blockers, operation=operation, retirement_sha256=retirement_sha256
        )
        with self._lock:
            if expected_binding is not None and (
                self._current is not expected_binding
                or self.closing
                or self._journal_unknown
                or self._core_shutdown_token is not None
                or any(row["state"] == "unknown" for row in self._operations.values())
            ):
                return
            self._recovery_cache = result

    def recovery_snapshot(self):
        with self._lock:
            return deepcopy(self._recovery_cache)

    def adoption_snapshot(self):
        with self._lock:
            return deepcopy(self._adoption_cache)

    def _root_exclusion(self, prior):
        actual = capture_managed_root_identity(
            self.root, self._core_instance, original_stream=self._core_stream
        )
        return (
            actual
            if (
                actual is not None
                and actual == self._managed_root_identity_sha256
                and actual == prior.get("managed_root_identity_sha256")
            )
            else None
        )

    def _classify_adoption(self, prior):
        observations, journal, delivery, policy = None, None, None, None
        try:
            self._proof_archive.verify(prior)
            observations = self._proof_archive.read_attachment(
                prior["original_observations_artifact_sha256"]
            )
            journal, delivery = read_default_carry_heads(self.root)
            policy = manual_startup_policy_sha256()
        except (
            StorageError,
            ValidationError,
            OSError,
            KeyError,
            TypeError,
            ValueError,
            SyntaxError,
        ):
            pass  # The pure classifier records absent evidence as blockers.
        exclusion = self._root_exclusion(prior)
        observation = classify_interrupted_cold_adoption(
            prior,
            journal,
            delivery,
            current_core_epoch=self.core_epoch,
            expected_policy_sha256=policy,
            current_root_owned=exclusion is not None,
            original_observations=observations,
        )
        try:
            unsettled = self._journal.recording_unknown or self._journal.has_unsettled_mutations()
        except (StorageError, ValidationError, OSError):
            unsettled = True
        if unsettled:
            observation = replace(
                observation,
                blockers=tuple(
                    sorted(set(observation.blockers) | {"COLD_FEATURE_MUTATIONS_UNSETTLED"})
                ),
            )
        cache = {
            **observation.document(),
            "observation_sha256": observation.sha256,
            "managed_root_matches": exclusion is not None,
        }
        with self._lock:
            self._adoption_cache = cache
        return observation, observations, exclusion

    def submit_adoption(self, identity_value, parameters):
        operation_id(identity_value)
        if type(parameters) is not dict or set(parameters) != {
            "core_epoch",
            "expected_observation_sha256",
            "state_policy",
            "confirm",
        }:
            raise ValidationError("exact cold adoption parameters required")
        identity(parameters["core_epoch"])
        checksum(parameters["expected_observation_sha256"])
        if parameters["state_policy"] != "reset" or parameters["confirm"] is not True:
            raise ValidationError("explicit confirmed cold reset adoption required")
        request_sha = digest(parameters)
        admission_serial = self.external_features.status().shutdown_serial
        with self._lock:
            previous = self._operations.get(identity_value)
            if previous is not None:
                if (
                    previous.get("kind") != "cold_adoption"
                    or previous["request_digest"] != request_sha
                ):
                    raise ValidationError("adoption ID conflicts with its original payload")
                return deepcopy(previous)
            if (
                self.thread is None
                or self.closed
                or self.closing
                or self._startup_error
                or self._core_shutdown_token is not None
                or self._current is not None
                or not self._adoption_cache["eligible"]
            ):
                raise TradingBlocked("cold namespace is not eligible for adoption")
            if (
                parameters["core_epoch"] != self.core_epoch
                or parameters["expected_observation_sha256"]
                != self._adoption_cache["observation_sha256"]
            ):
                raise ValidationError("cold adoption observation changed")
            if len(self._operations) >= 32 or any(
                row["state"] in {"admitted", "running", "unknown"}
                for row in self._operations.values()
            ):
                raise TradingBlocked("original adoption or recovery remains unsettled")
            if self._cold_prior_document and self._cold_prior_document.get("id") == identity_value:
                raise ValidationError("a new owner adoption requires a new operation ID")
            row = {
                "id": identity_value,
                "kind": "cold_adoption",
                "request_digest": request_sha,
                "state": "admitted",
                "durable": False,
                "error": None,
                "result": None,
            }
            try:
                self._queue.put_nowait(
                    _Ticket("adoption", (identity_value, deepcopy(parameters), admission_serial))
                )
            except queue.Full:
                raise TradingBlocked("cold adoption queue is full") from None
            self._operations[identity_value] = row
            return deepcopy(row)

    def notify_shutdown_requested(self, identity_value, active=True):
        """Memory-only Core interlock; release settlement remains available.

        The facade's short-lock transition is the install linearization point.
        Cancel clears only this matching token and never enables NN authority.
        """
        operation_id(identity_value)
        if type(active) is not bool:
            raise ValidationError("exact shutdown interlock flag required")
        with self._lock:
            previous = self._core_shutdown_token
            if self.closing or self.closed:
                raise TradingBlocked("host is already closing")
            if previous is not None and previous != identity_value:
                raise ValidationError("shutdown interlock belongs to another operation")
        if active:
            if self.external_features.fence_install(identity_value) is not True:
                raise ValidationError("original shutdown install fence conflicts")
        elif previous == identity_value:
            if self.external_features.clear_install_fence(identity_value) is not True:
                raise TradingBlocked("original shutdown install fence is unconfirmed")
        else:
            return False
        status = self.external_features.status()
        with self._lock:
            self._core_shutdown_token = identity_value if active else None
            cached = dict(self._recovery_cache)
            blockers = set(cached["blockers"]) - {"CORE_SHUTDOWN_REQUESTED"}
            if active:
                blockers.add("CORE_SHUTDOWN_REQUESTED")
            cached.update(
                core_shutdown_operation_id=self._core_shutdown_token,
                blockers=sorted(blockers),
                held=status.held,
                control_revision=status.control_revision,
            )
            self._recovery_cache = cached
        return True

    def recovery_status(self, identity_value):
        operation_id(identity_value)
        with self._lock:
            row = self._operations.get(identity_value)
            if row is None:
                raise ValidationError("causal recovery operation not found")
            return deepcopy(row)

    def submit_recovery(self, identity_value, parameters):
        operation_id(identity_value)
        request_sha = validate_recovery_parameters(parameters)
        parameters = decoded(encoded(parameters))
        with self._lock:
            previous = self._operations.get(identity_value)
            if previous is not None:
                if previous["request_digest"] != request_sha:
                    raise ValidationError("recovery ID conflicts with its original payload")
                return deepcopy(previous)
            if (
                self.thread is None
                or self.closing
                or self.closed
                or self._startup_error
                or self._core_shutdown_token is not None
            ):
                raise TradingBlocked("causal recovery host is unavailable")
            if len(self._operations) >= 32 or any(
                row["state"] in {"admitted", "running", "unknown"}
                for row in self._operations.values()
            ):
                raise TradingBlocked("original recovery operation remains unsettled")
            current = self._recovery_cache
            if (
                parameters["core_epoch"] != self.core_epoch
                or parameters["expected_generation"] != current["generation"]
                or parameters["expected_control_revision"] != current["control_revision"]
                or parameters["expected_actor_token"] != current["actor_token"]
                or parameters["expected_retirement_sha256"] != current["retirement_sha256"]
            ):
                raise ValidationError("causal recovery admission snapshot changed")
            row = {
                "id": identity_value,
                "request_digest": request_sha,
                "state": "admitted",
                "durable": False,
                "error": None,
                "result": None,
            }
            ticket = _Ticket("recovery", (identity_value, parameters))
            try:
                self._queue.put_nowait(ticket)
            except queue.Full:
                raise TradingBlocked("causal recovery queue is full") from None
            self._operations[identity_value] = row
            return deepcopy(row)

    def _request(self, kind, *arguments):
        ticket = _Ticket(kind, arguments)
        with self._lock:
            unavailable = self.thread is None or self.closed or self._startup_error is not None
        if unavailable:
            raise TradingBlocked("causal host coordinator is unavailable")
        try:
            self._queue.put_nowait(ticket)
        except queue.Full:
            raise FeatureMutationRejected("coordinator_queue_full") from None
        if not ticket.done.wait(self.ack_timeout_s):
            with ticket.lock:
                if not ticket.done.is_set():
                    ticket.abandoned = True
                    raise TimeoutError("coordinator RPC acknowledgement is unconfirmed")
        if ticket.error is not None:
            raise ticket.error
        return ticket.result

    def prepare_mutation(self, ref, args_json):
        return self._request("prepare", ref, args_json)

    def finish_mutation(self, ref, result_json):
        return self._request("terminal", ref, result_json)

    def finish_not_dispatched_mutation(self, ref, witness):
        return self._request("not_dispatched", ref, witness)

    def mark_unknown(self, ref, code):
        return self._request("unknown", ref, code)

    def lookup_mutation(self, ref):
        return self._request("lookup", ref)

    def consume_terminal(self, ref, ack, witness):
        if threading.current_thread() is self.thread:
            return self._consume_terminal(ref, ack, witness)
        return self._request("consume", ref, ack, witness)

    def reconcile_mutation(self, ref):
        return self._request("reconcile", ref)

    def start(self):
        with self._lock:
            if self.thread is not None or self.closing or self.closed:
                raise ValidationError("causal host cannot be started again")
            self.thread = threading.Thread(
                target=self._loop, name="Omega-InferenceHost", daemon=True
            )
            original = self.thread
        original.start()
        if not self._started.wait(self.ack_timeout_s):
            raise StorageError("causal host startup is unconfirmed")
        if self._startup_error is not None:
            raise StorageError("causal host journal or startup is unavailable")
        return self.snapshot()

    def _capture_current(self):
        with self._lock:
            return self._current

    def snapshot(self):
        binding = self._capture_current()
        result = deepcopy(self._empty_cache) if binding is None else binding.actor.snapshot()
        with self._lock:
            if binding is not self._current:
                result = deepcopy(self._empty_cache)
                binding = None
        result["server_time_ms"] = self.clock_ms()
        result["actor_generation"] = None if binding is None else binding.context.generation
        result["current_actor_token"] = None if binding is None else binding.context.actor_token
        with self._lock:
            captured = dict(self._controls)
        result["operations"] = [
            self._operation_overlay(row, captured.get(row.get("id"), (None, None))[0])
            for row in result.get("operations", ())
        ]
        return result

    def _operation_overlay(self, row, binding):
        result = deepcopy(row)
        current = self._capture_current()
        result["operation_actor_generation"] = (
            None if binding is None else binding.context.generation
        )
        result["operation_actor_token"] = None if binding is None else binding.context.actor_token
        result["current_actor_generation"] = None if current is None else current.context.generation
        return result

    def submit(self, identity_value, kind, parameters):
        operation_id(identity_value)
        parse_request(kind, parameters)
        return self._request("control", identity_value, kind, decoded(encoded(parameters)))

    def status(self, identity_value):
        operation_id(identity_value)
        with self._lock:
            record = self._controls.get(identity_value)
        if record is None:
            binding = self._capture_current()
            if binding is None:
                raise ValidationError("causal inference operation not found")
            # A cold-loaded actor journal does not establish a past host
            # generation. Preserve that absence instead of relabeling history.
            return self._operation_overlay(binding.actor.status(identity_value), None)
        binding, kind = record
        result = binding.actor.status(identity_value)
        # Both actor status and provider metadata are bounded cached memory.
        # Old completion can never enable or refresh a replacement binding.
        with self._lock:
            control_held = (
                self.closing
                or self._core_shutdown_token is not None
                or self._journal_unknown
                or any(row["state"] == "unknown" for row in self._operations.values())
            )
        if (
            not control_held
            and result.get("state") == "complete"
            and binding is self._capture_current()
        ):
            self.external_features.refresh_bindings(binding.facade_binding)
            if kind == "reset_stream" and not self._cold_settlement_blocked:
                status = self.external_features.status()
                if not binding.actor.snapshot().get("error"):
                    enabled = self.external_features.enable_current(
                        binding.context.generation,
                        binding.context.actor_token,
                        expected_control_revision=status.control_revision,
                    )
                    if enabled:
                        self._publish_recovery("ACTIVE", expected_binding=binding)
        return self._operation_overlay(result, binding)

    def _new_actor(self, generation, token, *, carry_install=None):
        context = GenerationContext(
            self.core_epoch,
            generation,
            token,
            self._host_token,
            self._journal.mutation_ack_authority(),
        )
        options = {} if carry_install is None else {"carry_install": carry_install}
        self._factory_attempted += 1
        actor = self._actor_factory(
            self.root,
            core_epoch=self.core_epoch,
            training_job_provider=self.training_job_provider,
            on_incident=self.on_incident,
            evidence_limits=self.evidence_limits,
            on_storage_warning=self.on_storage_warning,
            actor_token=token,
            generation_context=context,
            **options,
        )
        binding = _ActorBinding(
            context,
            actor,
            GenerationBinding(self.core_epoch, generation, token, actor.external_features),
        )
        self._retained_candidates.append(binding)  # Retain before uncertain start.
        return binding

    def _save_host(self, document, *, attachment=None):
        if self._journal_unknown or self._journal.recording_unknown:
            raise StorageError("original recovery recording remains unknown")
        revision, _ = self._journal.read_host()
        root_identity = capture_managed_root_identity(
            self.root, self._core_instance, original_stream=self._core_stream
        )
        document = {**document, "managed_root_identity_sha256": root_identity}
        self._current_journal_write_attempted = True
        try:
            record, complete, pending = self._proof_archive.prepare(
                document, self._host_document, attachment=attachment
            )
            pending["managed_root_identity_sha256"] = root_identity
            reservation = exact_ack(
                self._journal.save_host(encoded(pending), expected_revision=revision)
            )
            if (
                reservation.revision != revision + 1
                or self._journal.last_ack is not reservation
                or self._journal.recording_unknown
            ):
                raise StorageError("recovery proof reservation is unconfirmed")
            self._proof_archive.write(record)
            if (
                capture_managed_root_identity(
                    self.root, self._core_instance, original_stream=self._core_stream
                )
                != root_identity
            ):
                raise StorageError("managed Root identity changed during host recording")
            ack = exact_ack(
                self._journal.save_host(encoded(complete), expected_revision=reservation.revision)
            )
            if (
                ack.revision != reservation.revision + 1
                or self._journal.recording_unknown
                or self._journal.last_ack is not ack
            ):
                raise StorageError("recovery checkpoint acknowledgement is unconfirmed")
            actual_revision, actual = self._journal.read_host()
            if actual_revision != ack.revision or actual != encoded(complete):
                raise StorageError("recovery checkpoint current bytes differ from exact ACK")
        except BaseException:
            self._journal_unknown = True
            raise
        self._host_document, self._host_ack = complete, ack
        return ack

    def _initialize(self):
        self._journal = self._journal_factory(self.root, limits=self.evidence_limits)
        self._journal_scope = self._journal.lease_scope
        self._proof_archive = RecoveryProofArchive(self.root, self.evidence_limits)
        _, raw = self._journal.read_host()
        self._host_document = decoded(raw)
        prior = self._host_document
        generation = 0
        if prior:
            self._cold_prior_document = deepcopy(prior)
            if prior.get("phase") != "proof_pending" and "proof_artifact_sha256" in prior:
                try:
                    self._proof_archive.verify(prior)
                except (StorageError, ValidationError, OSError, ValueError, TypeError):
                    self._cold_prior_document = deepcopy(prior)
                    self._classify_adoption(prior)
                    self._publish_recovery("HELD", ("COLD_REGISTRY_MANUAL_RECONCILIATION",))
                    return
            # A clean phase enables NEW manual adoption under the current Core
            # lease. It does not prove any past process's physical close.
            lease = self._core_instance
            clean = (
                prior.get("phase") == "closed"
                and prior.get("manual_required") is True
                and type(prior.get("generation")) is int
                and 0 <= prior["generation"] < 2**63 - 1
                and type(lease) is InstanceLock
                and lease.stream is self._core_stream
                and self._core_stream.closed is False
                and Path(self._core_stream.name).absolute() == self.root / "state" / "core.lock"
            )
            if not clean:
                self._cold_prior_document = deepcopy(prior)
                self._classify_adoption(prior)
                self._publish_recovery("HELD", ("COLD_REGISTRY_MANUAL_RECONCILIATION",))
                return
            generation = prior["generation"] + 1
            try:
                if prior.get("current_carry_manifest_sha256") is not None:
                    self._initialize_clean_carry(prior, generation)
                    return
                _, delivery = read_default_carry_heads(self.root)
                if delivery["snapshot"]["pins"] or delivery["snapshot"]["reservations"]:
                    self._classify_adoption(prior)
                    self._publish_recovery("HELD", ("COLD_CLOSED_CARRY_EVIDENCE_MISSING",))
                    return
            except (StorageError, ValidationError, OSError, KeyError, TypeError, ValueError):
                if not self._current_journal_write_attempted and self._factory_attempted == 0:
                    self._classify_adoption(prior)
                    self._publish_recovery("HELD", ("COLD_CLOSED_EVIDENCE_UNCONFIRMED",))
                    return
                raise
        token = uuid4().hex
        self._save_host(
            {
                "phase": "initial_intent",
                "core_epoch": self.core_epoch,
                "generation": generation,
                "actor_token": token,
                "previous_clean_registry_sha256": digest(prior),
                "manual_required": True,
            }
        )
        binding = self._new_actor(generation, token)
        binding.actor.start()
        self.external_features.bind_initial(binding.facade_binding, held=True)
        with self._lock:
            self._current = binding
        startup = self._startup_ack(binding)
        membership = binding.actor.generation_settlement_snapshot()
        self._cold_settlement_blocked = (
            membership is None
            or membership.capture_failed
            or membership.recording_unknown
            or not membership.current_write_confirmed
            or bool(membership.pins)
            or bool(membership.reservations)
            or bool(membership.outcomes)
        )
        self._save_host(
            {
                "phase": "manual_adopt",
                "core_epoch": self.core_epoch,
                "generation": generation,
                "actor_token": token,
                "startup_ack": asdict(startup),
                "legacy_adoption": True,
                "past_close_ack_claimed": False,
                "manual_required": True,
            }
        )
        self._publish_recovery(
            "MANUAL_REQUIRED",
            ("HISTORICAL_PINS_REQUIRE_CARRY",) if self._cold_settlement_blocked else (),
        )

    def _owner_exclusion_document(self, prior, root_identity):
        return {
            "scope": "current_managed_parent_exclusion",
            "core_epoch": self.core_epoch,
            "managed_root_identity_sha256": root_identity,
            "prior_registry_sha256": digest(prior),
            "physical_close_claimed": False,
            "arbitrary_worker_cleanup_claimed": False,
            "old_uncertainty_cleared": False,
        }

    def _initialize_clean_carry(self, prior, generation):
        root_identity = self._root_exclusion(prior)
        if root_identity is None:
            self._cold_prior_document = deepcopy(prior)
            self._classify_adoption(prior)
            self._publish_recovery("HELD", ("CURRENT_ROOT_IDENTITY_UNCONFIRMED",))
            return
        observations = self._proof_archive.read_attachment(prior["proof_artifact_sha256"])
        validate_original_cold_observations(prior, observations)
        manifest = CarryManifest.parse(observations["carry_manifest"])
        if manifest.sha256 != prior["current_carry_manifest_sha256"] or (
            manifest.origin_core_epoch,
            manifest.origin_generation,
            manifest.origin_actor_token,
        ) != (prior["core_epoch"], prior["generation"], prior["actor_token"]):
            raise StorageError("current closed carry identity changed")
        verify_clean_closed_carry_manifest(self.root, manifest, limits=self.evidence_limits)
        exclusion = self._owner_exclusion_document(prior, root_identity)
        token, serial = uuid4().hex, self.external_features.status().shutdown_serial
        intent = {
            "phase": "initial_intent",
            "core_epoch": self.core_epoch,
            "generation": generation,
            "actor_token": token,
            "manual_required": True,
            "previous_clean_registry_sha256": digest(prior),
            "owner_exclusion": exclusion,
            "carry_manifest_sha256": manifest.sha256,
            "past_close_ack_claimed": False,
        }
        self._save_host(
            intent, attachment={"prior_registry": prior, "original_observations": observations}
        )
        if self._root_exclusion(prior) != root_identity:
            raise StorageError("managed Root exclusion changed before clean carry startup")
        verify_clean_closed_carry_manifest(self.root, manifest, limits=self.evidence_limits)
        self._start_adoption_candidate(
            intent, manifest, token, generation, serial, phase="manual_adopt"
        )

    def _begin_adoption(self, key, parameters, admission_serial):
        prior = self._cold_prior_document
        if (
            prior is None
            or self._current is not None
            or self.closing
            or self._core_shutdown_token
            or self.external_features.status().shutdown_serial != admission_serial
        ):
            self._set_operation(key, "blocked", code="COLD_ADOPTION_FENCE_CHANGED")
            return
        _, actual = self._journal.read_host()
        if actual != encoded(prior):
            self._set_operation(key, "blocked", code="COLD_REGISTRY_CHANGED")
            return
        observation, observations, root_identity = self._classify_adoption(prior)
        if (
            not observation.eligible
            or observation.sha256 != parameters["expected_observation_sha256"]
            or root_identity is None
        ):
            self._set_operation(key, "blocked", code="COLD_ADOPTION_OBSERVATION_CHANGED")
            return
        certificate = ManualStartupCertificate.parse(prior["candidate_startup_certificate"])
        exclusion = self._owner_exclusion_document(prior, root_identity)
        manifest = build_cold_carry_manifest(
            self.root, certificate, exclusion_sha256=digest(exclusion), limits=self.evidence_limits
        )
        token, serial = uuid4().hex, admission_serial
        generation = manifest.origin_generation + 1
        if generation >= 2**63:
            self._set_operation(key, "blocked", code="COLD_GENERATION_CAPACITY_EXHAUSTED")
            return
        intent = {
            "phase": "adoption_intent",
            "id": key,
            "kind": "cold_adoption",
            "request_sha256": digest(parameters),
            "parameters": parameters,
            "core_epoch": self.core_epoch,
            "generation": generation,
            "actor_token": token,
            "manual_required": True,
            "prior_registry_sha256": digest(prior),
            "prior_operation_id": prior.get("id"),
            "prior_phase": prior["phase"],
            "observation_sha256": observation.sha256,
            "owner_exclusion": exclusion,
            "carry_manifest_sha256": manifest.sha256,
            "past_close_ack_claimed": False,
            "old_request_replayed": False,
        }
        self._save_host(
            intent,
            attachment={
                "prior_registry": prior,
                "original_observations": observations,
                "carry_manifest": manifest.document(),
            },
        )
        self._set_operation(key, "running", durable=True)
        fresh, _, fresh_root = self._classify_adoption(prior)
        if (
            not fresh.eligible
            or fresh.sha256 != observation.sha256
            or fresh_root != root_identity
            or self.closing
            or self._core_shutdown_token is not None
            or self.external_features.status().shutdown_serial != serial
        ):
            raise StorageError("cold exclusion or startup heads changed before successor factory")
        self._start_adoption_candidate(
            intent, manifest, token, generation, serial, phase="adopted_manual", key=key
        )

    def _start_adoption_candidate(
        self, intent, manifest, token, generation, shutdown_serial, *, phase, key=None
    ):
        if self.closing or self._core_shutdown_token is not None:
            raise StorageError("cold binding is fenced by shutdown")
        candidate = self._new_actor(
            generation,
            token,
            carry_install=CarryInstallRequest(manifest, self.core_epoch, generation, token),
        )
        candidate.actor.start()
        startup = self._startup_ack(candidate)
        acceptance = candidate.actor.carry_acceptance_snapshot()
        verify_carry_acceptance(candidate, manifest, acceptance)
        prepared = {
            **intent,
            "phase": "adoption_prepared",
            "candidate_startup_ack": asdict(startup),
            "candidate_startup_certificate": startup_certificate_document(candidate, startup),
            "carry_acceptance": asdict(acceptance),
        }
        self._save_host(prepared)
        status = self.external_features.status()
        if (
            self.closing
            or self._core_shutdown_token is not None
            or status.shutdown_serial != shutdown_serial
            or not self.external_features.bind_cold_initial(
                candidate.facade_binding,
                manifest,
                acceptance,
                control_revision=status.control_revision,
            )
        ):
            raise StorageError("fresh cold binding CAS was fenced")
        with self._lock:
            self._current = candidate
        receipt = {
            "scope": "new_owner_manual_adoption",
            "core_epoch": self.core_epoch,
            "generation": generation,
            "actor_token": token,
            "new_actor_token": token,
            "prior_core_epoch": manifest.origin_core_epoch,
            "manifest_sha256": manifest.sha256,
            "startup_ack_sha256": startup.snapshot_sha256,
            "acceptance_sha256": acceptance.sha256,
            "owner_exclusion_sha256": digest(intent["owner_exclusion"]),
            "automatic_inference_started": False,
            "financial_resumed": False,
            "old_error_cleared": False,
            "old_request_replayed": False,
            "old_physical_close_claimed": False,
            "manual_required": True,
        }
        self._save_host({**prepared, "phase": phase, "adoption_receipt": receipt})
        self._carry_available, self._carry_manifest_sha256 = True, manifest.sha256
        self._last_carry_counts = {
            "pins": len(manifest.pins),
            "reservations": len(manifest.reservations),
            "unknown_reservations": sum(
                row["state"] == "unknown"
                for row in decoded(manifest.provenance_json)["reservations"]
            ),
        }
        self._cold_settlement_blocked = False
        if key is not None:
            self._set_operation(
                key,
                "succeeded",
                durable=True,
                result={**receipt, "receipt_sha256": digest(receipt)},
            )
        with self._lock:
            self._adoption_cache = {
                **self._adoption_cache,
                "eligible": False,
                "blockers": [],
                "state": "adopted_manual",
            }
        self._publish_recovery("MANUAL_REQUIRED")

    @staticmethod
    def _startup_ack(binding):
        retirement = binding.actor.retirement_snapshot()
        if (
            retirement is None
            or retirement.actor_token != binding.context.actor_token
            or retirement.core_epoch != binding.context.core_epoch
            or retirement.capture_failed
            or retirement.recording_unknown
            or retirement.journal_write_pending
            or not retirement.factory_coverage_complete
            or not retirement.identity_intact
        ):
            raise StorageError("candidate current startup journal coverage is unconfirmed")
        return exact_ack(retirement.last_journal_ack)

    def _set_operation(self, key, state, *, durable=False, code=None, result=None):
        with self._lock:
            self._operations[key].update(
                state=state,
                durable=durable,
                error=None if code is None else {"code": code},
                result=result,
            )

    def _begin_recovery(self, key, parameters):
        binding = self._capture_current()
        status = self.external_features.status()
        if (
            binding is None
            or self.closing
            or self._core_shutdown_token is not None
            or parameters["expected_generation"] != status.generation
            or parameters["expected_actor_token"] != status.actor_token
            or parameters["expected_control_revision"] != status.control_revision
        ):
            self._set_operation(key, "blocked", code="RECOVERY_CAS_CHANGED")
            return
        token = uuid4().hex
        request_sha = digest(parameters)
        intent = {
            "phase": "intent",
            "id": key,
            "request_sha256": request_sha,
            "core_epoch": self.core_epoch,
            "generation": binding.context.generation,
            "actor_token": binding.context.actor_token,
            "candidate_actor_token": token,
            "parameters": parameters,
        }
        ack = self._save_host(intent)
        fence = self.external_features.hold(request_sha)
        self._recovery = {
            "id": key,
            "binding": binding,
            "fence": fence,
            "token": token,
            "intent": intent,
            "intent_ack": ack,
        }
        self._set_operation(key, "running", durable=True)
        self._publish_recovery("RECOVERY_HELD", operation=key)

    def _owner_settlement(self, binding, ref, *, no_effect=False):
        outcome = binding.actor.read_feature_settlement(ref, host_token=self._host_token)
        return self._validate_owner_settlement(binding, ref, outcome, no_effect=no_effect)

    def _validate_owner_settlement(self, binding, ref, outcome, *, no_effect=False):
        if outcome is None:
            return None
        if type(outcome) is not FeatureSettlementSnapshot:
            return None
        valid_phase = (
            outcome.phase == "rejected"
            and outcome.result_json is None
            and outcome.error_code == "original_mutation_rejected"
            and digest(decoded(outcome.arguments_json)) == ref.arguments_sha256
            if no_effect
            else outcome.phase == "complete" and outcome.result_json is not None
        )
        if (
            outcome.ref != ref
            or outcome.context_sha256 != binding.context.binding_sha256
            or outcome.recording_unknown
            or outcome.capture_failed
            or not outcome.current_write_confirmed
            or not valid_phase
        ):
            return None
        ack = exact_ack(outcome.last_exact_ack)
        if ack.revision != outcome.head_revision or ack.snapshot_sha256 != outcome.head_sha256:
            return None
        return outcome

    def _finish_not_dispatched(self, ref, witness):
        binding = self._capture_current()
        if (
            type(ref) is not FeatureMutationRef
            or binding is None
            or not binding.context.matches(ref)
            or not self.external_features.is_current_binding(binding.facade_binding)
            or self.external_features.verify_no_effect_witness(witness, ref) is not True
        ):
            raise ValidationError("exact original no-effect rejection witness required")
        previous = self._not_dispatched.get(ref.sha256)
        if previous is not None:
            if previous[0] is not binding or previous[1] != ref or previous[3] is not witness:
                raise ValidationError("original no-effect evidence conflicts")
            return previous[2]
        if len(self._not_dispatched) >= 128:
            raise FeatureMutationUnknown(ref, "terminal", False)
        membership = binding.actor.generation_settlement_snapshot()
        if (
            type(membership) is not GenerationSettlementSnapshot
            or membership.context_sha256 != binding.context.binding_sha256
            or membership.recording_unknown
            or membership.capture_failed
            or not membership.current_write_confirmed
        ):
            raise FeatureMutationUnknown(ref, "terminal", False)
        head_ack = exact_ack(membership.last_exact_ack)
        if (
            head_ack.revision != membership.head_revision
            or head_ack.snapshot_sha256 != membership.head_sha256
        ):
            raise FeatureMutationUnknown(ref, "terminal", False)
        # A direct pre-ticket rejection has no actor outcome. An actor-owned
        # rejection instead keeps its original ACK/confirmation obligations.
        outcome = binding.actor.read_feature_settlement(ref, host_token=self._host_token)
        if (
            outcome is not None
            and self._validate_owner_settlement(binding, ref, outcome, no_effect=True) is None
        ):
            raise FeatureMutationUnknown(ref, "terminal", False)
        self._current_journal_write_attempted = True
        ack = self._journal.finish_mutation(ref, encoded({"not_dispatched": True}))
        if not binding.context.verifies_terminal_ack(ref, ack, ack.result_json):
            raise FeatureMutationUnknown(ref, "terminal", False)
        self._not_dispatched[ref.sha256] = (binding, ref, ack, witness, outcome is not None)
        if outcome is not None:
            self._ack_owner(binding, ref, ack, no_effect=True)
        return ack

    def _ack_owner(self, binding, ref, ack, *, no_effect=False):
        original = binding.actor.thread
        owner_confirmation_required = original is not None and original.is_alive()
        if owner_confirmation_required:
            try:
                result = binding.actor.ack_feature_settlement(ref, ack, host_token=self._host_token)
                if result is not True:
                    raise FeatureMutationUnknown(ref, "terminal", False)
            except FeatureMutationUnknown:
                raise
            except BaseException as error:
                # The terminal journal ACK remains issued. Losing the actor's
                # cache-retirement RPC says nothing about underlying store IO.
                raise FeatureMutationUnknown(ref, "terminal", False) from error
        else:
            outcome = self._owner_settlement(binding, ref, no_effect=no_effect)
            result_json = (
                encoded({"not_dispatched": True})
                if no_effect
                else (None if outcome is None else outcome.result_json)
            )
            if (
                outcome is None
                or result_json != ack.result_json
                or not binding.context.verifies_terminal_ack(ref, ack, result_json)
            ):
                raise FeatureMutationUnknown(ref, "terminal", False)
            # This is a NEW host terminal archive, never an ACK from the dead
            # actor. Its original immutable outcome remains in the old cache.
            key = ref.sha256
            covered = dict(self._terminal_coverage)
            covered[key] = outcome.sha256
            if len(covered) > 128:
                raise FeatureMutationUnknown(ref, "terminal", False)
            self._save_host({**self._host_document, "terminal_coverage": sorted(covered.items())})
            self._terminal_coverage = covered
        if (
            ref.sha256 not in self._completed_terminal
            and len(self._completed_terminal) >= 128
            or not binding.context.verifies_terminal_ack(ref, ack, ack.result_json)
        ):
            raise FeatureMutationUnknown(ref, "terminal", False)
        self._completed_terminal[ref.sha256] = (
            binding,
            ref,
            ack,
            owner_confirmation_required,
            no_effect,
        )

    def _retire_terminal(self, ref):
        witness = self._completed_terminal.get(ref.sha256)
        if witness is None or witness[1] != ref:
            return False
        binding, _, ack, owner_confirmation_required, no_effect = witness
        if owner_confirmation_required:
            try:
                original = binding.actor.thread
                if original is None or not original.is_alive():
                    self._ack_owner(binding, ref, ack, no_effect=no_effect)
                elif (
                    binding.actor.confirm_feature_settlement_ack(
                        ref, ack, host_token=self._host_token
                    )
                    is not True
                ):
                    return False
            except BaseException:
                return False  # Retain the exact positive witness and issuer.
        if self._journal.retire_mutation_ack(ref, ack) is not True:
            return False
        del self._completed_terminal[ref.sha256]
        return True

    def _consume_terminal(self, ref, ack, witness):
        binding = self._capture_current()
        if (
            binding is None
            or not binding.context.matches(ref)
            or not self.external_features.verify_applied_mutation(witness, ref, ack)
        ):
            return False
        previous = self._consumed_terminal.get(ref.sha256)
        if previous is not None:
            return (
                previous[0] is binding
                and previous[1] == ref
                and previous[2] is ack
                and previous[3] is witness
            )
        if len(self._consumed_terminal) >= 128 or not binding.context.verifies_terminal_ack(
            ref, ack, ack.result_json
        ):
            return False
        not_dispatched = self._not_dispatched.get(ref.sha256)
        if not_dispatched is not None:
            original, exact_ref, issued, no_effect_witness, owner_required = not_dispatched
            if original is not binding or exact_ref != ref or issued is not ack:
                return False
            if (
                no_effect_witness is not None
                and self.external_features.verify_no_effect_witness(no_effect_witness, ref)
                is not True
            ):
                return False
            if owner_required:
                if ref.sha256 not in self._completed_terminal:
                    self._ack_owner(binding, ref, ack, no_effect=True)
                if not self._retire_terminal(ref):
                    return False
            elif self._journal.retire_mutation_ack(ref, ack) is not True:
                return False
            del self._not_dispatched[ref.sha256]
        else:
            positive = self._completed_terminal.get(ref.sha256)
            if positive is None:
                self._ack_owner(binding, ref, ack)
            elif positive[0] is not binding or positive[1] != ref or positive[2] is not ack:
                return False
            if not self._retire_terminal(ref):
                return False
        self._consumed_terminal[ref.sha256] = (binding, ref, ack, witness)
        return True

    def _retire_settled_terminals(self):
        pending = frozenset(self.external_features.pending_mutations())
        for key, (_, ref, _, _) in tuple(self._consumed_terminal.items()):
            if ref not in pending:
                del self._consumed_terminal[key]
        for _, ref, _, _, _ in tuple(self._completed_terminal.values()):
            if ref not in pending:
                self._retire_terminal(ref)

    def _reconcile(self, ref):
        binding = self._capture_current()
        if binding is None or not binding.context.matches(ref):
            return False
        consumed = self._consumed_terminal.get(ref.sha256)
        if consumed is not None:
            return self.external_features.reconcile_mutation(ref, consumed[2])
        not_dispatched = self._not_dispatched.get(ref.sha256)
        if not_dispatched is not None:
            original, exact_ref, ack, no_effect_witness, _ = not_dispatched
            if (
                original is not binding
                or exact_ref != ref
                or (not binding.context.verifies_terminal_ack(ref, ack, ack.result_json))
            ):
                return False
            if no_effect_witness is None:
                return self.external_features.settle_not_dispatched(ref, ack)
            if self.external_features.verify_no_effect_witness(no_effect_witness, ref) is not True:
                return False
            return self.external_features.settle_not_dispatched(ref, ack, witness=no_effect_witness)
        witness = self._completed_terminal.get(ref.sha256)
        if witness is not None:
            original, exact_ref, ack, _, _ = witness
            if (
                original is binding
                and exact_ref == ref
                and binding.context.verifies_terminal_ack(ref, ack, ack.result_json)
                and self.external_features.reconcile_mutation(ref, ack)
            ):
                self._retire_terminal(ref)
                return True
            return False
        record = self._journal.get_mutation(ref)
        if record is not None and record.ack.state == "terminal":
            if decoded(record.ack.result_json) == {"not_dispatched": True}:
                return False  # Persisted JSON alone is never original no-effect authority.
        outcome = self._owner_settlement(binding, ref)
        if outcome is None:
            return False
        ack = self._journal.finish_mutation(ref, outcome.result_json)
        self._ack_owner(binding, ref, ack)
        if not self.external_features.reconcile_mutation(ref, ack):
            return False
        self._retire_terminal(ref)
        return True

    def _advance_recovery(self):
        work = self._recovery
        if work is None:
            return
        key, binding = work["id"], work["binding"]
        status = self.external_features.status()
        if status.borrows or status.entry_leases:
            return  # The loop continues serving borrower journal RPCs.
        for ref in self.external_features.pending_mutations():
            self._reconcile(ref)
        self._retire_settled_terminals()
        status = self.external_features.status()
        if status.hot_mutations or status.recording_unknown:
            self._block_recovery("ORIGINAL_MUTATIONS_OR_PINS_UNSETTLED")
            return
        if self._completed_terminal:
            self._block_recovery("OWNER_TERMINAL_ACK_CONFIRM_UNCONFIRMED")
            return
        if not self.external_features.is_current_binding(binding.facade_binding):
            self._block_recovery("ORIGINAL_MUTATIONS_OR_PINS_UNSETTLED")
            return
        if (
            self.closing
            or self._core_shutdown_token is not None
            or not self.external_features.close_settlement(work["fence"])
        ):
            self._block_recovery("HOST_CLOSING_OR_FENCE_CHANGED")
            return
        # Seal while live; an already dead actor supplies only its original final cache.
        actor = binding.actor
        if actor.thread is not None and actor.thread.is_alive():
            actor.seal_feature_settlement(host_token=self._host_token)
        close_error = None
        try:
            actor.close()
        except StorageError as error:
            close_error = error
        retirement = actor.retirement_snapshot()
        join = actor.actor_join_observation(timeout_s=0)
        settlement = actor.generation_settlement_snapshot()
        carry_required = settlement is not None and bool(settlement.pins or settlement.reservations)
        proof_builder = (
            retired_generation_carry_proof if carry_required else retired_generation_proof
        )
        proof = proof_builder(
            binding.context, retirement, join, settlement, tuple(self._terminal_coverage.items())
        )
        facade_documents = self.external_features.settlement_pin_documents()
        sealed_documents = tuple(sorted(encoded(pin.document()) for pin in settlement.pins))
        if (
            type(facade_documents) is not tuple
            or len(set(facade_documents)) != len(facade_documents)
            or tuple(sorted(facade_documents)) != sealed_documents
        ):
            self._block_recovery("ORIGINAL_MUTATIONS_OR_PINS_UNSETTLED")
            return
        head = verified_journal_head(self.root, retirement.last_journal_ack)
        observations = original_observation_document(retirement, join, settlement, proof)
        observations_artifact_sha256 = None
        manifest = None
        if carry_required:
            manifest = build_carry_manifest(
                self.root,
                context=binding.context,
                physical_proof_sha256=proof.sha256,
                journal_ack=retirement.last_journal_ack,
                settlement=settlement,
                limits=self.evidence_limits,
                terminal_coverage=tuple(self._terminal_coverage.items()),
            )
            if (
                type(manifest) is not CarryManifest
                or manifest.recording_unknown
                or manifest.unresolved_outcomes
                or manifest.physical_proof_sha256 != proof.sha256
                or (
                    manifest.origin_core_epoch,
                    manifest.origin_generation,
                    manifest.origin_actor_token,
                )
                != (
                    binding.context.core_epoch,
                    binding.context.generation,
                    binding.context.actor_token,
                )
                or manifest.origin_provider_token != settlement.provider_token
                or manifest.origin_store_scope_token != settlement.store_scope_token
                or manifest.origin_store_binding_token != settlement.store_binding_token
                or manifest.journal_revision != proof.journal_revision
                or manifest.journal_sha256 != proof.journal_sha256
                or manifest.settlement_sha256 != settlement.sha256
                or tuple(sorted(encoded(pin.document()) for pin in manifest.pins))
                != tuple(sorted(encoded(pin.document()) for pin in settlement.pins))
                or tuple(sorted(manifest.reservations)) != tuple(sorted(settlement.reservations))
                or tuple(sorted(facade_documents))
                != tuple(sorted(encoded(pin.document()) for pin in manifest.pins))
            ):
                raise StorageError("exact original carry membership is unconfirmed")
            carry_summary = {
                "manifest_sha256": manifest.sha256,
                "pins": len(manifest.pins),
                "reservations": len(manifest.reservations),
                "delivery_revision": manifest.delivery_revision,
                "delivery_sha256": manifest.delivery_sha256,
                "charged_records": manifest.charged_records,
                "charged_bytes": manifest.charged_bytes,
                "unknown_reservations": sum(
                    row["state"] == "unknown"
                    for row in decoded(manifest.provenance_json)["reservations"]
                ),
            }
            self._save_host(
                {**work["intent"], "phase": "carry_manifest", "carry": carry_summary},
                attachment={**observations, "carry_manifest": manifest.document()},
            )
            observations_artifact_sha256 = self._host_document["proof_artifact_sha256"]
        self._history.append((binding, retirement, join, settlement, proof, close_error))
        self._publish_recovery(
            "OLD_RETIRED", operation=key, retirement_sha256=proof.retirement_sha256
        )
        # The actual old head can truthfully remain ready/running after a fault.
        paused = all(
            row.get("state") in {"manual_required", "paused", "retired", "failed"}
            for row in head.get("deployments", {}).values()
        )
        pending = pending_unknown_evidence(head)
        pending_sha = digest(pending)
        mode = (
            "old_pause_ack"
            if paused
            and not proof.original_errors
            and not pending["operations"]
            and not pending["streams"]
            else "host_fault_barrier"
        )
        barrier_document = {
            **work["intent"],
            "phase": "fault_barrier",
            "proof": proof.document(),
            "mode": mode,
            "pending_unknown": pending,
            "pending_unknown_sha256": pending_sha,
            "settlement_sha256": settlement.sha256,
        }
        if manifest is not None:
            barrier_document["carry"] = carry_summary
        barrier_ack = self._save_host(
            barrier_document, attachment=observations if manifest is None else None
        )
        if observations_artifact_sha256 is None:
            observations_artifact_sha256 = self._host_document["proof_artifact_sha256"]
        barrier = FaultPauseBarrierAck(
            work["intent"]["request_sha256"],
            proof.sha256,
            mode,
            proof.journal_revision,
            proof.journal_sha256,
            proof.journal_sha256 if mode == "old_pause_ack" else None,
            settlement.sha256,
            pending_sha,
            proof.original_errors,
            barrier_ack.revision,
            barrier_ack.snapshot_sha256,
        )
        if self.closing or self._core_shutdown_token is not None:
            self._block_recovery("HOST_CLOSING")
            return
        carry_install = (
            None
            if manifest is None
            else CarryInstallRequest(
                manifest, self.core_epoch, binding.context.generation + 1, work["token"]
            )
        )
        candidate = self._new_actor(
            binding.context.generation + 1, work["token"], carry_install=carry_install
        )
        work["candidate"] = candidate
        candidate.actor.start()
        startup = self._startup_ack(candidate)
        acceptance = None
        if manifest is not None:
            acceptance = candidate.actor.carry_acceptance_snapshot()
            verify_carry_acceptance(candidate, manifest, acceptance)
        view = candidate.actor.snapshot()
        if any(not row.get("manual_required") for row in view.get("streams", ())):
            raise StorageError("candidate startup requires new manual stream ACK")
        prepared = {
            **barrier_document,
            "phase": "candidate_prepared",
            "barrier": barrier.document(),
            "candidate_startup_ack": asdict(startup),
            "candidate_startup_certificate": startup_certificate_document(candidate, startup),
            "original_observations_artifact_sha256": observations_artifact_sha256,
        }
        if acceptance is not None:
            prepared["carry_acceptance"] = asdict(acceptance)
        self._save_host(prepared)
        # Public close fences the facade before this short pointer CAS.
        installed = False
        if not self.closing:
            installed = (
                self.external_features.install_cas(work["fence"], candidate.facade_binding)
                if manifest is None
                else self.external_features.install_carry_cas(
                    work["fence"], candidate.facade_binding, manifest, acceptance
                )
            )
        if not installed:
            self._block_recovery("INSTALL_CAS_REJECTED")
            return
        with self._lock:
            self._current = candidate
        status = self.external_features.status()
        receipt = RestorationReceipt(
            work["intent"]["request_sha256"],
            self.core_epoch,
            binding.context.generation,
            candidate.context.generation,
            binding.context.actor_token,
            candidate.context.actor_token,
            work["intent_ack"].snapshot_sha256,
            barrier.sha256,
            startup.snapshot_sha256,
            digest({"namespace": "original_causal_state"}),
            digest(view.get("deployments", [])),
            status.control_revision,
            settlement_policy=(
                "verified_historical_pin_carry"
                if manifest is not None
                else "verified_pins0_reservations0"
            ),
        )
        self._save_host({**prepared, "phase": "restored_manual", "receipt": receipt.document()})
        if manifest is not None:
            self._carry_available, self._carry_manifest_sha256 = True, manifest.sha256
            self._last_carry_counts = {
                name: carry_summary[name]
                for name in ("pins", "reservations", "unknown_reservations")
            }
            self._cold_settlement_blocked = False
        self._set_operation(
            key,
            "succeeded",
            durable=True,
            result={
                "receipt_sha256": receipt.sha256,
                "generation": candidate.context.generation,
                "manual_required": True,
                "historical_carry_available": self._carry_available,
                "carry_manifest_sha256": self._carry_manifest_sha256,
            },
        )
        self._recovery = None
        self._publish_recovery("MANUAL_REQUIRED", retirement_sha256=proof.retirement_sha256)

    def _block_recovery(self, code, *, unknown=False):
        if self._recovery is None:
            return
        key = self._recovery["id"]
        unknown |= "candidate" in self._recovery
        self._set_operation(key, "unknown" if unknown else "blocked", code=code)
        self._publish_recovery("UNKNOWN" if unknown else "HELD", (code,), operation=key)
        self._recovery = None

    def _serve(self, ticket):
        result = None
        try:
            self._retire_settled_terminals()
            kind, args = ticket.kind, ticket.arguments
            if kind == "prepare":
                ref, arguments = args
                binding = self._capture_current()
                if (
                    type(ref) is not FeatureMutationRef
                    or binding is None
                    or not binding.context.matches(ref)
                    or ref not in self.external_features.pending_mutations()
                ):
                    raise ValidationError("exact original mutation reference required")
                self._current_journal_write_attempted = True
                result = self._journal.prepare_mutation(ref, arguments)
                with ticket.lock:
                    if ticket.abandoned:
                        result = self._journal.finish_mutation(
                            ref, encoded({"not_dispatched": True})
                        )
                        self._not_dispatched[ref.sha256] = (binding, ref, result, None, False)
            elif kind == "not_dispatched":
                result = self._finish_not_dispatched(*args)
            elif kind == "terminal":
                if decoded(args[1]) == {"not_dispatched": True}:
                    raise ValidationError(
                        "original no-effect witness required before terminal write"
                    )
                self._current_journal_write_attempted = True
                result = self._journal.finish_mutation(*args)
                binding = self._capture_current()
                if binding is None or not binding.context.matches(args[0]):
                    raise FeatureMutationUnknown(args[0], "terminal", False)
                self._ack_owner(binding, args[0], result)
            elif kind == "unknown":
                record = self._journal.get_mutation(args[0])
                if record is None or record.ack.state != "terminal":
                    self._current_journal_write_attempted = True
                result = (
                    record.ack
                    if record is not None and record.ack.state == "terminal"
                    else self._journal.mark_unknown(*args)
                )
            elif kind == "lookup":
                result = self._journal.get_mutation(*args)
            elif kind == "consume":
                result = self._consume_terminal(*args)
            elif kind == "reconcile":
                result = self._reconcile(*args)
            elif kind == "control":
                key, command, parameters = args
                binding = self._capture_current()
                with self._lock:
                    unknown = self._journal_unknown or any(
                        row["state"] == "unknown" for row in self._operations.values()
                    )
                if (
                    binding is None
                    or self.closing
                    or self._core_shutdown_token is not None
                    or self._recovery is not None
                    or unknown
                ):
                    raise TradingBlocked("current generation is held for recovery")
                held = self.external_features.status().held
                if held and (
                    command == "predict_committed"
                    or command == "reset_stream"
                    and parameters.get("state_policy") != "reset"
                ):
                    raise TradingBlocked("new manual reset stream is required")
                with self._lock:
                    previous = self._controls.get(key)
                    if previous is not None and previous[0] is not binding:
                        raise ValidationError("operation ID belongs to an older generation")
                    if len(self._controls) >= 256 and previous is None:
                        raise TradingBlocked("host control evidence capacity reached")
                    self._controls[key] = (binding, command)
                result = self._operation_overlay(
                    binding.actor.submit(key, command, parameters), binding
                )
            elif kind == "recovery":
                self._begin_recovery(*args)
            elif kind == "adoption":
                self._begin_adoption(*args)
            elif kind == "close":
                self._shutdown_requested = True
            elif kind == "preserve_close":
                self._preserve_shutdown_requested = True
            else:
                raise ValidationError("unknown host coordinator request")
        except BaseException as error:
            ticket.error = error
            if ticket.kind in {"recovery", "adoption"}:
                self._set_operation(
                    ticket.arguments[0], "unknown", code="RECOVERY_RECORDING_UNCONFIRMED"
                )
                self._publish_recovery("UNKNOWN", ("RECOVERY_RECORDING_UNCONFIRMED",))
        finally:
            ticket.result = result
            self._publish_ticket(ticket)

    @staticmethod
    def _publish_ticket(ticket):
        with ticket.lock:
            ticket.done.set()

    def _shutdown_step(self):
        status = self.external_features.status()
        if status.borrows or status.entry_leases:
            return False
        if status.hot_mutations:
            for ref in self.external_features.pending_mutations():
                self._reconcile(ref)
            if self.external_features.status().hot_mutations:
                raise StorageError("original mutation shutdown remains unsettled")
        self._retire_settled_terminals()
        if self._completed_terminal:
            raise StorageError("original terminal acknowledgement confirmation remains unsettled")
        physical = True
        for binding in self._retained_candidates:
            try:
                binding.actor.close()
            except StorageError as error:
                self._close_errors.append((binding, error))
            retirement, join = (
                binding.actor.retirement_snapshot(),
                binding.actor.actor_join_observation(timeout_s=0),
            )
            if retirement is not None:
                validate_retirement_value(retirement)
            if join is not None:
                validate_retirement_value(join)
            if (
                retirement is None
                or not retirement.sealed
                or not retirement.physical_confirmed
                or retirement.capture_failed
                or retirement.recording_unknown
                or retirement.journal_write_pending
                or join is None
                or not join.original_thread_dead
                or not join.identity_intact
            ):
                physical = False
        with self._lock:
            unsettled = any(
                row["state"] in {"admitted", "running", "unknown"}
                for row in self._operations.values()
            )
        if (
            not physical
            or self._journal_unknown
            or self._journal.recording_unknown
            or unsettled
            or self._startup_error is not None
            or self._capture_current() is None
        ):
            raise StorageError("host original physical or durable cleanup remains unconfirmed")
        current = self._capture_current()
        retirement = current.actor.retirement_snapshot()
        join = current.actor.actor_join_observation(timeout_s=0)
        settlement = current.actor.generation_settlement_snapshot()
        carry = settlement is not None and bool(settlement.pins or settlement.reservations)
        builder = retired_generation_carry_proof if carry else retired_generation_proof
        proof = builder(
            current.context, retirement, join, settlement, tuple(self._terminal_coverage.items())
        )
        if tuple(sorted(self.external_features.settlement_pin_documents())) != tuple(
            sorted(encoded(pin.document()) for pin in settlement.pins)
        ):
            raise StorageError("current shutdown pin membership is unconfirmed")
        observations = original_observation_document(retirement, join, settlement, proof)
        head = verified_journal_head(self.root, retirement.last_journal_ack)
        closed = {
            **self._host_document,
            "phase": "closed",
            "manual_required": True,
            "core_epoch": self.core_epoch,
            "generation": current.context.generation,
            "actor_token": current.context.actor_token,
            "proof": proof.document(),
            "pending_unknown": pending_unknown_evidence(head),
            "cold_adoption_scope": "new_manual_current_core_root_lease",
            "past_close_ack_claimed": False,
            "current_carry_manifest_sha256": None,
        }
        closed.pop("original_observations_artifact_sha256", None)
        closed.pop("carry_acceptance", None)
        if carry:
            manifest = build_carry_manifest(
                self.root,
                context=current.context,
                physical_proof_sha256=proof.sha256,
                journal_ack=retirement.last_journal_ack,
                settlement=settlement,
                limits=self.evidence_limits,
                terminal_coverage=tuple(self._terminal_coverage.items()),
            )
            observations["carry_manifest"] = manifest.document()
            closed["current_carry_manifest_sha256"] = manifest.sha256
        closed["pending_unknown_sha256"] = digest(closed["pending_unknown"])
        self._save_host(closed, attachment=observations)
        self._journal_retirement = self._journal.close()
        if not self._journal_retirement.all_confirmed or self._journal.recording_unknown:
            raise StorageError("original recovery journal file cleanup remains unconfirmed")
        with self._lock:
            self.closed = True
        self._publish_recovery("CLOSED")
        return True

    def can_close_preserving_cold_namespace(self):
        """Cached admission hint; only the explicit physical receipt is positive."""
        with self._lock:
            return self._preservation_preconditions()

    def _preservation_preconditions(self):
        return (
            self._cold_prior_document is not None
            and self._current is None
            and self._factory_attempted == 0
            and not self._retained_candidates
            and self._host_ack is None
            and self._journal is not None
            and self._startup_error is None
            and not self._journal_unknown
            and not self._current_journal_write_attempted
            and self._recovery is None
            and not self._completed_terminal
            and not self._consumed_terminal
            and not self._not_dispatched
            and all(row["state"] == "blocked" for row in self._operations.values())
        )

    def _preservation_receipt(self):
        return {
            **FLAGS,
            "closed": True,
            "durable": False,
            "actor_alive": False,
            "worker_alive": False,
            "namespace_preserved": True,
            "namespace_clean": False,
            "current_physical_cleanup_confirmed": True,
            "current_required_recording_state": "none_attempted",
            "old_resource_close_claimed": False,
            "current_journal_close_sha256": digest(asdict(self._journal_retirement)),
        }

    def _preserve_shutdown_step(self):
        with self._lock:
            allowed = self._preservation_preconditions()
        if not allowed:
            raise StorageError("current cold namespace preservation preconditions are unconfirmed")
        self._journal_retirement = self._journal.close()
        validate_retirement_value(self._journal_retirement)
        if not self._journal_retirement.all_confirmed:
            raise StorageError("current cold journal file cleanup is unconfirmed")
        with self._lock:
            self.closed, self._namespace_preserved = True, True
        self._publish_recovery("COLD_NAMESPACE_PRESERVED", ("COLD_NAMESPACE_NOT_CLEAN",))
        return True

    def close_preserving_cold_namespace(self):
        if not self.can_close_preserving_cold_namespace():
            raise StorageError("cold namespace preservation is not available")
        self.external_features.fence_shutdown()
        with self._lock:
            self.closing = True
            original = self.thread
        if original is None or original is threading.current_thread():
            raise StorageError("current coordinator cleanup cannot be observed here")
        if original.is_alive():
            try:
                self._queue.put_nowait(_Ticket("preserve_close", ()))
            except queue.Full:
                raise StorageError("cold preservation shutdown queue is full") from None
            original.join(timeout=self.close_timeout_s)
        if original.is_alive():
            raise StorageError("current coordinator cleanup deadline exceeded")
        # A preceding default close may already have positively closed this
        # current journal scope while refusing to bless the old NN namespace.
        receipt = self._journal_retirement
        if receipt is None:
            raise StorageError("current cold journal close receipt is absent")
        validate_retirement_value(receipt)
        if not receipt.all_confirmed:
            raise StorageError("current cold journal cleanup is unconfirmed")
        with self._lock:
            if not self._preservation_preconditions():
                raise StorageError("current preservation fence changed")
            self.closed, self._namespace_preserved = True, True
        return self._preservation_receipt()

    def _loop(self):
        try:
            try:
                self._initialize()
            except BaseException as error:
                self._startup_error = error
                self._journal_scope = getattr(error, "lease_scope", self._journal_scope)
                self._publish_recovery("UNKNOWN", ("HOST_STARTUP_UNCONFIRMED",))
            finally:
                self._started.set()
            while True:
                active = (
                    self._recovery is not None
                    or self._shutdown_requested
                    or self._preserve_shutdown_requested
                )
                try:
                    ticket = self._queue.get(timeout=0.05 if active else None)
                except queue.Empty:
                    ticket = None
                if ticket is not None:
                    self._serve(ticket)
                try:
                    if self._preserve_shutdown_requested:
                        if self._preserve_shutdown_step():
                            break
                    elif self._shutdown_requested:
                        if self._shutdown_step():
                            break
                    elif self._recovery is not None:
                        self._advance_recovery()
                except BaseException:
                    if self._shutdown_requested or self._preserve_shutdown_requested:
                        self._publish_recovery("UNKNOWN", ("HOST_SHUTDOWN_UNCONFIRMED",))
                        break
                    self._block_recovery("ORIGINAL_RECOVERY_EVIDENCE_UNCONFIRMED", unknown=True)
                    # Preserve this operation/candidate but do not spin or retry it.
                    self._recovery = None
        finally:
            # Resource cleanup remains mandatory even when durable publication
            # failed. A successful file close cannot clear the sticky failure.
            if self._journal_retirement is None:
                try:
                    if self._journal is not None:
                        self._journal_retirement = self._journal.close()
                    elif self._journal_scope is not None:
                        self._journal_retirement = self._journal_scope.close_all()
                except BaseException:
                    self._journal_unknown = True
            self._finished.set()

    def close(self):
        # Linearize shutdown before any candidate pointer CAS; no callbacks or
        # IO occur inside this facade transition.
        self.external_features.fence_shutdown()
        with self._lock:
            if self._namespace_preserved:
                raise StorageError("preserved cold namespace has no clean shutdown receipt")
            never_started = (
                self.thread is None
                and self._factory_attempted == 0
                and not self._started.is_set()
                and not self._finished.is_set()
                and self._journal is None
                and self._journal_scope is None
                and self._journal_retirement is None
                and self._proof_archive is None
                and self._current is None
                and not self._retained_candidates
                and not self._history
                and not self._operations
                and not self._controls
                and not self._completed_terminal
                and not self._consumed_terminal
                and not self._not_dispatched
                and not self._terminal_coverage
                and self._host_ack is None
                and not self._host_document
                and self._recovery is None
                and self._queue.empty()
                and self._startup_error is None
                and not self._journal_unknown
            )
            if never_started:
                self._never_started_closed = True
                self.closed, self.closing = True, True
                self._recovery_cache = {**self._recovery_cache, "state": "CLOSED", "held": True}
            if self.closed:
                return {
                    **FLAGS,
                    "closed": True,
                    "durable": True,
                    "actor_alive": False,
                    "worker_alive": False,
                    "evidence_basis": (
                        "never_started_no_acquisitions"
                        if self._never_started_closed
                        else "original_owner_shutdown_receipts"
                    ),
                    "durable_recording_required": not self._never_started_closed,
                    "physical_cleanup_required": not self._never_started_closed,
                }
            self.closing = True
            original = self.thread
        if original is None:
            raise StorageError("unstarted host has no original journal shutdown proof")
        if original is threading.current_thread():
            raise StorageError("host cannot confirm cleanup from its own callback")
        if original.is_alive():
            try:
                self._queue.put_nowait(_Ticket("close", ()))
            except queue.Full:
                raise StorageError("host shutdown admission is unconfirmed") from None
            original.join(timeout=self.close_timeout_s)
        if original.is_alive() or not self.closed:
            raise StorageError("host original cleanup acknowledgement is unconfirmed")
        return {
            **FLAGS,
            "closed": True,
            "durable": True,
            "actor_alive": False,
            "worker_alive": False,
        }
