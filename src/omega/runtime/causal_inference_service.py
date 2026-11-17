"""Single-thread owner of explicit committed inference; no automatic TS delivery.

The HTTP/GUI side only queues controls and reads bounded cached views. All model
verification, DB reads, file ACKs, recurrent state changes and cleanup belong to
one actor thread; model execution itself remains in the existing isolated child.
"""

# Imports
import threading
import time
from collections import deque
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.domain.inference import (
    CAUSAL_SCOPE,
    InferenceCommitAck,
    InferenceModelIdentity,
    decoded,
    digest,
    encoded,
    identity,
    timestamp,
)
from omega.domain.model_contract import ShadowModelContract
from omega.infrastructure.market_data import instrument_key
from omega.runtime.causal_actor_retirement import ActorJoinObserver, ActorOriginalResources
from omega.runtime.causal_capacity import EvidenceBudgetLimits, evidence_storage_unavailable
from omega.runtime.causal_feature_delivery import CausalFeatureDeliveryProvider
from omega.runtime.causal_generation_outcomes import GenerationContext
from omega.runtime.causal_historical_carry import CarryInstallRequest
from omega.runtime.causal_inference_ports import ObservedInferenceWorker
from omega.runtime.causal_inference_registry import (
    FLAGS,
    TERMINAL,
    operation_id,
    parse_bindings,
    parse_request,
    registered_training,
    validate_journal,
)
from omega.runtime.causal_market_source import (
    CommittedInferenceMarketSource,
    SourceReadRetirementSnapshot,
)
from omega.runtime.causal_nn import CausalNNFeatureOwner, FileInferenceStateStore
from omega.runtime.causal_nn_proof import (
    InferenceActivation,
    InferenceInputRegistry,
    prepare_causal_activation,
    read_document,
    wall_ms,
)
from omega.runtime.causal_nn_worker import CausalInferenceProcessWorker
from omega.runtime.causal_startup_policy import capture_manual_startup_certificate
from omega.runtime.causal_store_leases import (
    InferenceStoreLeases,
    StoreLeaseBinding,
    StoreLeaseSnapshot,
)


def failure(code):
    return {
        "code": code,
        "message": "Committed inference needs explicit verification or recovery.",
        "manual_required": True,
    }


def feature_view(value):
    return {
        "request_id": value.request_id,
        "input_sha256": value.input_sha256,
        "frame_sha256": value.frame_sha256,
        "open_time_ms": value.open_time_ms,
        "decision_at_ms": value.decision_at_ms,
        "accepted_at_ms": value.accepted_at_ms,
        "valid_until_ms": value.valid_until_ms,
        "values": {name: str(number) for name, number in value.values},
    }


class CausalInferenceService:
    def __init__(
        self,
        root,
        *,
        core_epoch,
        training_job_provider,
        source_factory=None,
        worker_factory=None,
        owner_factory=None,
        store_factory=None,
        clock_ms=None,
        on_incident=None,
        ack_timeout_s=5,
        close_timeout_s=15,
        evidence_limits=None,
        on_storage_warning=None,
        actor_token=None,
        generation_context=None,
        carry_install=None,
    ):
        self.root, self.core_epoch = Path(root).absolute(), identity(core_epoch)
        if (
            not callable(training_job_provider)
            or not 0 < ack_timeout_s <= 30
            or not 0 < close_timeout_s <= 60
            or evidence_limits is not None
            and type(evidence_limits) is not EvidenceBudgetLimits
            or on_storage_warning is not None
            and not callable(on_storage_warning)
        ):
            raise ValidationError("bounded causal service timeouts and job provider required")
        self.training_job_provider = training_job_provider
        self.source_factory = source_factory or CommittedInferenceMarketSource
        self.worker_factory = worker_factory or CausalInferenceProcessWorker
        self.owner_factory = owner_factory or CausalNNFeatureOwner
        self.store_factory = store_factory or FileInferenceStateStore
        self._default_owner_factory = owner_factory is None
        self._default_store_factory = store_factory is None
        self._default_source_factory = source_factory is None
        self._default_worker_factory = worker_factory is None
        if generation_context is not None:
            if (
                type(generation_context) is not GenerationContext
                or generation_context.core_epoch != self.core_epoch
            ):
                raise ValidationError("exact original causal generation context required")
            if actor_token is not None and actor_token != generation_context.actor_token:
                raise ValidationError("reserved causal actor token differs")
            actor_token = generation_context.actor_token
        self._generation_context = generation_context
        if carry_install is not None and (
            type(carry_install) is not CarryInstallRequest
            or generation_context is None
            or (
                carry_install.new_core_epoch,
                carry_install.new_generation,
                carry_install.new_actor_token,
            )
            != (
                generation_context.core_epoch,
                generation_context.generation,
                generation_context.actor_token,
            )
        ):
            raise ValidationError("carry install differs from reserved original generation")
        self._carry_install = carry_install
        self._manual_startup_certificate = None
        self._actor_token = uuid4().hex if actor_token is None else identity(actor_token)
        self._actor_thread_token = uuid4().hex
        self._original_actor_thread = self._actor_join_observer = None
        self._original_resources = self._retirement_cache = None
        self._retirement_capture_failed = self._retirement_recording_unknown = False
        self._retirement_write_pending = False
        self._retirement_last_ack = None
        self._original_journal_store = None
        self._original_journal_binding = None
        self._file_leases = self._file_lease_cache = None
        self.clock_ms, self.on_incident = clock_ms or wall_ms, on_incident
        self.ack_timeout_s, self.close_timeout_s = ack_timeout_s, close_timeout_s
        self.evidence_limits = evidence_limits or EvidenceBudgetLimits()
        self.on_storage_warning = on_storage_warning
        self._storage_warning_seen, self._storage_warnings = set(), set()
        self._evidence_storage = evidence_storage_unavailable(self.evidence_limits)
        self.condition = threading.Condition(threading.RLock())
        self.thread, self.store, self.owner, self.source, self.activation = (
            None,
            None,
            None,
            None,
            None,
        )
        self.registry, self.active = None, None
        self._unattached_worker = None
        self._quarantined, self._retained_resources = [], []
        self._cleanup_retry, self._cleanup_consumed = 0, 0
        self._admissions, self._work, self._tickets, self._pending = deque(), deque(), {}, {}
        self._requests = {}
        self._receipt_available = False
        self._feature_commands = deque()
        self._feature_events = deque()
        self._dispatch_feature_event = None
        self.external_features = CausalFeatureDeliveryProvider(self)
        self._dispatch_operation = None
        self._data, self._stored_revision = None, None
        self._incidents = set()
        self.ready, self.closing, self.closed, self.error = False, False, False, None
        self._cache = self._empty()

    def _now(self):
        return timestamp(self.clock_ms())

    def _empty(self):
        return {
            **FLAGS,
            "schema_version": 1,
            "core_epoch": self.core_epoch,
            "revision": 0,
            "state": "disabled",
            "ready": False,
            "storage_available": True,
            "evidence_storage": deepcopy(self._evidence_storage),
            "actor_alive": False,
            "server_time_ms": 0,
            "active_deployment_id": None,
            "deployments": [],
            "streams": [],
            "operations": [],
            "limits": {
                "active_deployments": 1,
                "streams": 128,
                "worker_queue": 8,
                "control_queue": 32,
                "operation_history": 256,
                "deployment_history": 32,
            },
            "error": None,
        }

    def _save(self, *, publish=True):
        candidate = deepcopy(self._data)
        candidate["revision"] += 1
        raw = encoded(candidate)
        self._retirement_write_pending = True
        try:
            try:
                ack = self.store.save_snapshot(raw, expected_revision=self._stored_revision)
                if (
                    not ack.durable
                    or ack.revision != candidate["revision"]
                    or ack.snapshot_sha256 != digest(candidate)
                ):
                    raise StorageError("causal service journal ACK unconfirmed")
            except Exception:
                self._fault("CAUSAL_INFERENCE_RECORDING_UNCONFIRMED")
                raise StorageError("causal service recording is unconfirmed") from None
            self._stored_revision, self._data = candidate["revision"], candidate
            # Existing injected duck ACKs retain their original save behavior.
            # Only exact acknowledged data belongs in the new internal evidence.
            self._record_journal_ack(ack)
        except BaseException:
            self._retirement_recording_unknown = True
            raise
        else:
            self._retirement_write_pending = False
        if publish:
            self._publish()

    def _record_journal_ack(self, ack):
        try:
            binding = self._original_journal_binding
            original_default = (
                self._default_store_factory
                and type(self.store) is FileInferenceStateStore
                and self.store is self._original_journal_store
            )
            if not original_default:
                return
            if (
                type(ack) is InferenceCommitAck
                and type(binding) is StoreLeaseBinding
                and self.store._lease_scope is self._file_leases
                and self.store._lease_binding is binding
                and binding.original_store is self.store
                and binding.snapshot().last_ack is ack
            ):
                self._retirement_last_ack = ack
            else:
                # A previously typed ACK does not prove the next successful
                # legacy-shaped save. Preserve the existing save result while
                # withholding a complete retirement cache for this actor.
                self._retirement_capture_failed = True
        except BaseException:
            self._retirement_capture_failed = True

    def _fault(self, code):
        self.error, self.ready = failure(code), False
        if self._data is not None:
            for row in self._data["operations"].values():
                if row["state"] not in TERMINAL:
                    row.update(state="unknown", durable=False, error=failure(code))
        self._publish()
        self._incidents.add(code)

    def _deliver_incidents(self):
        # submit() can be awaited while the HTTP caller owns the core fence.
        # Every admission waiter is signalled before a callback can acquire it.
        if self.on_incident is not None:
            for code in sorted(self._incidents):
                try:
                    self.on_incident(code)
                except Exception:
                    pass  # Storage uncertainty remains visible in the cached status.
        self._incidents.clear()

    def _evidence_changed(self, observation):
        """Store actor callback: cache only, no incident callback under ACK/cache locks."""
        self._evidence_storage = deepcopy(observation)
        if observation["state"] in {"warning", "full"}:
            code = (
                "NN_EVIDENCE_CAPACITY_FULL"
                if observation["state"] == "full"
                else "NN_EVIDENCE_CAPACITY_WARNING"
            )
            if code not in self._storage_warning_seen:
                self._storage_warning_seen.add(code)
                self._storage_warnings.add(code)
        with self.condition:
            self._cache["evidence_storage"] = (
                evidence_storage_unavailable(self.evidence_limits)
                if self.error or self.closed or self.closing
                else deepcopy(observation)
            )
            self.condition.notify_all()

    def _deliver_storage_warnings(self):
        # Admission/feature waiters have been signalled before calling into core.
        warnings, self._storage_warnings = sorted(self._storage_warnings), set()
        if self.on_storage_warning is not None:
            for code in warnings:
                try:
                    self.on_storage_warning(code)
                except Exception:
                    pass  # Advisory delivery never changes durable model/financial state.

    def _publish(self):
        if self._file_leases is not None and self.thread is threading.current_thread():
            try:
                file_observation = {
                    "scope": "original_inference_file_stores_only",
                    "snapshot": asdict(self._file_leases.snapshot()),
                    "complete_default_file_factories": (
                        self._default_store_factory and self._default_owner_factory
                    ),
                    "restoration_authorized": False,
                }
            except BaseException:
                file_observation = {
                    "scope": "original_inference_file_stores_only",
                    "state": "unavailable",
                    "restoration_authorized": False,
                }
                self.error = failure("CAUSAL_INFERENCE_FILE_LEASE_OBSERVATION_UNCONFIRMED")
                self.ready = False
                self._incidents.add("CAUSAL_INFERENCE_FILE_LEASE_OBSERVATION_UNCONFIRMED")
            with self.condition:
                self._file_lease_cache = file_observation
        result = self._empty()
        result.update(
            ready=self.ready,
            storage_available=self.error is None,
            active_deployment_id=self.active,
            error=deepcopy(self.error),
        )
        if self._data is not None:
            result["revision"] = self._data["control_revision"]
            for row in self._data["deployments"].values():
                model = row["model"] or {}
                result["deployments"].append(
                    {
                        "id": row["id"],
                        "state": row["state"],
                        "training_job_id": row["training_job_id"],
                        "module_id": model.get("module_id"),
                        "model_revision": model.get("revision"),
                        "module_sha256": model.get("module_sha256"),
                        "checkpoint_sha256": model.get("checkpoint_sha256"),
                        "available_at_ms": model.get("available_at_ms"),
                        "training_cutoff_ms": model.get("training_cutoff_ms"),
                        "contract": deepcopy(row["contract"]),
                        "bindings": deepcopy(row["bindings"]),
                        "manual_required": row["state"] != "ready",
                        "error": deepcopy(row["error"]),
                    }
                )
                result["streams"].extend(deepcopy(list(row["streams"].values())))
            result["operations"] = [
                self._operation_view(row) for row in self._data["operations"].values()
            ]
            states = [row["state"] for row in self._data["deployments"].values()]
            result["state"] = (
                "ready"
                if self.active
                else "manual_required"
                if "manual_required" in states
                else "paused"
                if "paused" in states
                else "retired"
                if states and set(states) == {"retired"}
                else "disabled"
            )
        if self.error:
            result["state"] = "recording_unconfirmed"
        elif self.closed:
            result["state"] = "closed"
        if self.error or self.closed or self.closing:
            result["evidence_storage"] = evidence_storage_unavailable(self.evidence_limits)
        if self.thread is threading.current_thread():
            self.external_features._sync_actor()
        with self.condition:
            self._cache = result
            self.condition.notify_all()

    @staticmethod
    def _operation_view(row):
        return {
            **FLAGS,
            **{key: deepcopy(value) for key, value in row.items() if key != "parameters"},
        }

    def snapshot(self):
        now = self._now()  # Clock and bounded memory only; never a model/file/DB.
        with self.condition:
            result = deepcopy(self._cache)
            result.update(
                actor_alive=bool(self.thread and self.thread.is_alive()), server_time_ms=now
            )
        for row in result["streams"]:
            value = row["feature"]
            if value is not None and (
                self.error is not None
                or not value["accepted_at_ms"] <= now < value["valid_until_ms"]
                or row["manual_required"]
            ):
                row["feature"] = None
        return result

    def status(self, identity_value):
        operation_id(identity_value)
        with self.condition:
            for row in self._cache["operations"]:
                if row["id"] == identity_value:
                    return deepcopy(row)
            ticket = self._tickets.get(identity_value)
            if ticket is not None and ticket.get("timeout"):
                return deepcopy(ticket["timeout"])
        raise ValidationError("causal inference operation not found")

    def start(self):
        with self.condition:
            if self.thread is not None or self.closed or self.closing:
                raise ValidationError("causal inference actor cannot be started again")
            self.thread = threading.Thread(
                target=self._loop, name="Omega-CommittedInference", daemon=True
            )
            self._original_actor_thread = self.thread
            self._actor_join_observer = ActorJoinObserver(
                self._actor_token, self._actor_thread_token, self.thread
            )
            self.thread.start()
            end = time.monotonic() + self.ack_timeout_s
            while not self.ready and self.error is None and self.thread.is_alive():
                remaining = end - time.monotonic()
                if remaining <= 0:
                    raise StorageError("causal service startup is unconfirmed")
                self.condition.wait(remaining)
            if self.error or not self.ready:
                raise StorageError("causal service registry unavailable")
        return self.snapshot()

    def submit(self, identity_value, kind, parameters):
        operation_id(identity_value)
        request_digest = parse_request(kind, parameters)
        parameters = decoded(encoded(parameters, maximum=65536))
        with self.condition:
            previous = next(
                (row for row in self._cache["operations"] if row["id"] == identity_value), None
            )
            ticket = self._tickets.get(identity_value)
            original = previous or ticket
            if original:
                if original["request_digest"] != request_digest:
                    raise ValidationError("causal operation ID conflicts with its original payload")
                if previous:
                    return deepcopy(previous)
            else:
                if not self.ready or self.closed or self.closing or self.error:
                    raise TradingBlocked("causal service is not ready for controls")
                if len(self._tickets) + len(self._work) >= 32:
                    raise ValidationError("causal service control queue is full")
                ticket = {
                    "id": identity_value,
                    "kind": kind,
                    "parameters": parameters,
                    "request_digest": request_digest,
                    "ack": None,
                    "error": None,
                    "abandoned": False,
                    "timeout": None,
                }
                self._tickets[identity_value] = ticket
                self._admissions.append(ticket)
                self.condition.notify_all()
            end = time.monotonic() + self.ack_timeout_s
            while ticket["ack"] is None and ticket["error"] is None:
                remaining = end - time.monotonic()
                if remaining <= 0:
                    ticket["abandoned"] = True
                    ticket["timeout"] = {
                        **FLAGS,
                        "id": identity_value,
                        "kind": kind,
                        "request_digest": request_digest,
                        "core_epoch": self.core_epoch,
                        "submitted_revision": parameters["revision"],
                        "revision": parameters["revision"],
                        "state": "unknown",
                        "durable": False,
                        "result": None,
                        "error": failure("ADMISSION_ACK_UNCONFIRMED"),
                        "created_at_ms": self._now(),
                        "started_at_ms": None,
                        "finished_at_ms": None,
                    }
                    return deepcopy(ticket["timeout"])
                self.condition.wait(remaining)
            if ticket["error"] is not None:
                raise ticket["error"]
            return deepcopy(ticket["ack"])

    def _initialize(self):
        # Construct on the original actor, before a factory can acquire a lease
        # and fail without returning its half-constructed store.
        self._original_resources = ActorOriginalResources(
            self._actor_token,
            self.core_epoch,
            self._original_actor_thread,
            owner_thread_token=self._actor_thread_token,
        )
        self._file_leases = InferenceStoreLeases(uuid4().hex)
        self._original_resources.bind_store_scope(
            self._file_leases,
            default_store_factory=(
                self._default_store_factory and self.store_factory is FileInferenceStateStore
            ),
        )
        path = self.root / "state/causal-inference.json"
        self.store = (
            self.store_factory(path, leases=self._file_leases, role="journal")
            if self._default_store_factory
            else self.store_factory(path)
        )
        self._original_journal_store = self.store
        if self._default_store_factory and type(self.store) is FileInferenceStateStore:
            self._original_journal_binding = self._file_leases.binding_for(self.store)
        raw = self.store.read()
        if raw is None:
            self._data = {
                "scope": CAUSAL_SCOPE,
                "schema_version": 1,
                "revision": 0,
                "core_epoch": self.core_epoch,
                "control_revision": 0,
                "deployments": {},
                "operations": {},
            }
        else:
            self._data = validate_journal(decoded(raw))
            self._stored_revision = self._data["revision"]
            for row in self._data["operations"].values():
                if row["state"] not in TERMINAL:
                    row.update(
                        state="unknown",
                        error=failure("RESTART_REQUIRES_MANUAL_RECOVERY"),
                        finished_at_ms=self._now(),
                    )
            for row in self._data["deployments"].values():
                if row["state"] == "preparing":
                    row.update(state="failed", error=failure("ACTIVATION_INCOMPLETE_AFTER_RESTART"))
                elif row["state"] not in {"retired", "failed"}:
                    row["state"] = "manual_required"
                for stream in row["streams"].values():
                    if stream["pending_operation_id"]:
                        row["uncertain_streams"] = sorted(
                            set(row["uncertain_streams"]) | {stream["stream"]}
                        )
                    stream.update(
                        state="manual_required",
                        manual_required=True,
                        pending_operation_id=None,
                        feature=None,
                        reason="restart_manual_required",
                    )
        self._data["core_epoch"] = self.core_epoch
        self._data["control_revision"] += 1
        self.external_features._start_actor()
        self._save()
        if self._generation_context is not None:
            self._cache_retirement(sealed=False)
            self._manual_startup_certificate = capture_manual_startup_certificate(self)
        self.ready = True
        self._publish()

    def _admit(self, ticket):
        try:
            parameters = ticket["parameters"]
            if (
                parameters["core_epoch"] != self.core_epoch
                or parameters["revision"] != self._data["control_revision"]
            ):
                raise ValidationError("causal operation epoch or revision conflict")
            capacity = 256 if ticket["kind"] in {"pause", "retire"} else 254
            if len(self._data["operations"]) >= capacity:
                raise ValidationError(
                    "causal operation history capacity reached; no ID was discarded"
                )
            revision = self._data["control_revision"] + 1
            row = {
                "id": ticket["id"],
                "kind": ticket["kind"],
                "parameters": parameters,
                "request_digest": ticket["request_digest"],
                "core_epoch": self.core_epoch,
                "submitted_revision": parameters["revision"],
                "revision": revision,
                "state": "unknown" if ticket["abandoned"] else "queued",
                "created_at_ms": self._now(),
                "started_at_ms": None,
                "finished_at_ms": None,
                "durable": True,
                "result": None,
                "error": failure("ADMISSION_ACK_UNCONFIRMED") if ticket["abandoned"] else None,
            }
            candidate = deepcopy(self._data)
            candidate["operations"][row["id"]] = row
            candidate["control_revision"] = revision
            encoded(candidate)
            self._data = candidate
            self._save(publish=False)  # Do not expose queued after a caller's timeout.
            with self.condition:
                abandoned = ticket["abandoned"]
                if not abandoned:
                    self._work.append(row["id"])
                    self._publish()
                    ticket["ack"] = self._operation_view(self._data["operations"][row["id"]])
            if abandoned and row["state"] == "queued":
                self._finish(row["id"], "unknown", code="ADMISSION_ACK_UNCONFIRMED")
            if abandoned:
                with self.condition:
                    ticket["ack"] = self._operation_view(self._data["operations"][row["id"]])
        except Exception as error:
            with self.condition:
                ticket["error"] = error
        finally:
            with self.condition:
                self._tickets.pop(ticket["id"], None)
                self.condition.notify_all()

    def _finish(self, key, state, *, result=None, code=None):
        self._data["operations"][key].update(
            state=state,
            finished_at_ms=self._now(),
            error=None if code is None else failure(code),
        )
        if result is not None:
            self._data["operations"][key]["result"] = result
        self._save()

    def _deployment(self, parameters):
        row = self._data["deployments"].get(parameters["deployment_id"])
        if row is None or row["state"] in {"failed", "retired", "preparing"}:
            raise TradingBlocked("causal deployment requires explicit activation")
        return row

    def _build_owner(self, row):
        originals = self._original_resources
        source_key, worker_key, owner_key = originals.reserve_run()
        activation_root = self.root / "artifacts/causal-inference" / row["id"] / "activation"
        if self.activation is None:
            self.activation = InferenceActivation(
                activation_root,
                row["manifest_sha256"],
                InferenceModelIdentity(**row["model"]),
                ShadowModelContract.parse(row["contract"]),
            )
            self.activation.verify()
        session_root = (
            self.root / "artifacts/causal-inference" / row["id"] / ("session-" + uuid4().hex)
        )
        source_factory = self.source_factory
        originals.enter(
            source_key,
            source_factory,
            default=(
                self._default_source_factory and source_factory is CommittedInferenceMarketSource
            ),
        )
        try:
            self.source = source_factory(self.root)
            originals.returned(source_key, self.source)
        except BaseException:
            self._retirement_acquisition_failed(source_key)
            raise
        self.registry = InferenceInputRegistry(
            session_root / "inputs", self.activation, self.source, clock_ms=self.clock_ms
        )
        worker_factory = self.worker_factory
        originals.enter(
            worker_key,
            worker_factory,
            default=(
                self._default_worker_factory and worker_factory is CausalInferenceProcessWorker
            ),
        )
        try:
            worker = worker_factory(self.activation, self.registry.root, session_root / "execution")
            self._unattached_worker = worker
            originals.returned(worker_key, worker)
        except BaseException:
            self._retirement_acquisition_failed(worker_key)
            raise
        raw_worker = worker
        attach_wakeup = getattr(worker, "set_wakeup", None)
        if callable(attach_wakeup):
            attach_wakeup(self._receipt_arrived)
        worker = ObservedInferenceWorker(
            worker, self._before_dispatch, self.external_features._receipt_actor
        )
        self._unattached_worker = worker
        originals.attach_wrapper(worker_key, worker)
        # Inspect actual prior pending evidence before owner.restore intentionally
        # clears it. Never claim such a state was safe to preserve automatically.
        path = self.root / "state/causal-nn" / (digest(row["id"]) + ".json")
        if path.exists():
            before = read_document(path)
            pending = before.get("snapshot", {}).get("pending", {})
            if type(pending) is not dict:
                worker.close()
                raise StorageError("saved inference pending state is unavailable")
            row["uncertain_streams"] = sorted(
                set(row["uncertain_streams"]) | {value["stream"] for value in pending.values()}
            )
        lease_options = {"leases": self._file_leases} if self._default_owner_factory else {}
        owner_factory = self.owner_factory
        originals.enter(
            owner_key,
            owner_factory,
            default=(self._default_owner_factory and owner_factory is CausalNNFeatureOwner),
        )
        try:
            self.owner = owner_factory(
                self.root,
                self.activation.model,
                parse_bindings(row["bindings"]),
                worker=worker,
                evidence=self.registry,
                core_epoch=self.core_epoch,
                deployment_epoch=row["id"],
                namespace=row["id"],
                clock_ms=self.clock_ms,
                on_incident=self._fault,
                **lease_options,
            )
            binding = (
                self._file_leases.binding_for(self.owner.store)
                if type(self.owner) is CausalNNFeatureOwner and self._default_owner_factory
                else None
            )
            originals.returned(
                owner_key,
                self.owner,
                related_worker=raw_worker,
                wrapper=worker,
                store_binding=binding,
            )
        except BaseException:
            self._retirement_acquisition_failed(owner_key)
            raise
        self._unattached_worker = None
        self.active = row["id"]
        row["state"], row["error"] = "ready", None
        self._refresh_streams(row)
        self.external_features._restore_actor()

    def _retirement_acquisition_failed(self, key):
        try:
            self._original_resources.acquisition_failed(key)
        except BaseException:
            self._retirement_capture_failed = True

    def _activate(self, parameters):
        if self.owner is not None or self.active is not None:
            raise TradingBlocked("retire or pause the active causal deployment first")
        source = registered_training(
            self.root, parameters["training_job_id"], self.training_job_provider
        )
        key = parameters["deployment_id"]
        if key is None:
            if len(self._data["deployments"]) >= 32:
                raise ValidationError("causal deployment history capacity reached")
            key = uuid4().hex
            row = {
                "id": key,
                "state": "preparing",
                "training_job_id": parameters["training_job_id"],
                "model": None,
                "manifest_sha256": None,
                "contract": parameters["contract"],
                "bindings": parameters["bindings"],
                "streams": {},
                "uncertain_streams": [],
                "error": None,
            }
            self._data["deployments"][key] = row
            self._save()  # Preserve partial creation intent; never reuse its directory.
            self.activation = prepare_causal_activation(
                source,
                self.root / "artifacts/causal-inference" / key / "activation",
                ShadowModelContract.parse(parameters["contract"]),
                trusted_module=True,
            )
            row = self._data["deployments"][key]
            row.update(
                model=self.activation.model.document(),
                manifest_sha256=self.activation.manifest_sha256,
            )
            self._save()
        else:
            row = self._deployment(parameters)
            if (
                row["training_job_id"] != parameters["training_job_id"]
                or row["contract"] != parameters["contract"]
                or row["bindings"] != parameters["bindings"]
            ):
                raise ValidationError("saved causal deployment configuration differs")
            self.activation = None
        row = self._data["deployments"][key]
        self._build_owner(row)
        self._save()
        return {"deployment_id": key}

    def _reset(self, parameters):
        row = self._deployment(parameters)
        if self.active not in {None, row["id"]}:
            raise TradingBlocked("another causal deployment is active")
        if parameters["state_policy"] == "reset" and (
            self._pending or parameters["stream"] in row["uncertain_streams"]
        ):
            self._suspend()
            row = self._data["deployments"][parameters["deployment_id"]]
        if self.owner is None:
            self.activation = None
            self._build_owner(row)
        stream = parameters["stream"]
        current = self.owner.snapshot()["state"]["streams"].get(stream)
        if parameters["state_policy"] == "preserve":
            if (
                current is None
                or stream in row["uncertain_streams"]
                or current["instrument"] != parameters["instrument"]
                or any(
                    value["stream"] == stream
                    for value in self.owner.snapshot()["state"]["pending"].values()
                )
            ):
                raise TradingBlocked("preserve requires an actual known saved stream state/cursor")
            state, cursor = current["state_json"], current["last_open_ms"]
        else:
            state, cursor = "null", None
        epoch = uuid4().hex
        self.owner.open_stream(
            stream,
            instrument_key(parameters["instrument"]),
            stream_epoch=epoch,
            initial_state_json=state,
            after_open_ms=cursor,
        )
        row["uncertain_streams"] = [value for value in row["uncertain_streams"] if value != stream]
        self._refresh_streams(row)
        self._save()
        return {"deployment_id": row["id"], "stream": stream, "stream_epoch": epoch}

    def _predict(self, parameters, key):
        row = self._deployment(parameters)
        if self.active != row["id"] or self.owner is None:
            raise TradingBlocked("causal deployment requires explicit activation/reset")
        stream = row["streams"].get(parameters["stream"])
        if (
            stream is None
            or stream["manual_required"]
            or stream["instrument"] != parameters["instrument"]
        ):
            raise TradingBlocked("causal stream requires explicit matching reset")
        self.external_features.store.preflight()
        frame = self.registry.frame(
            instrument_key(parameters["instrument"]), parameters["open_time_ms"]
        )
        if self.closing:
            raise TradingBlocked("causal actor is closing")
        decision = self._now()  # Actual source read and snapshot completed first.
        self._dispatch_operation = key
        try:
            request = self.owner.request(
                parameters["stream"],
                frame,
                decision_at_ms=decision,
                deadline_ms=decision + parameters["timeout_ms"],
            )
        except Exception:
            row = self._data["deployments"][parameters["deployment_id"]]
            actual = self.owner.snapshot()["state"]
            current = actual["streams"].get(parameters["stream"], {})
            reference = self._data["operations"][key]["result"] if key is not None else None
            if (
                key is None
                and self._dispatch_feature_event is not None
                and self._dispatch_feature_event["request_id"]
            ):
                reference = {"request_id": self._dispatch_feature_event["request_id"]}
            if current.get("health") == "blocked" or reference is not None:
                row["uncertain_streams"] = sorted(
                    set(row["uncertain_streams"]) | {parameters["stream"]}
                )
                self._refresh_streams(row)
                if self.error is None:
                    self._save()
                else:
                    self._publish()
            if reference:
                self._requests.pop(reference["request_id"], None)
                self.external_features._forgot_actor(reference["request_id"])
            raise
        finally:
            self._dispatch_operation = None
        self._pending[request.request_id] = key, parameters["stream"], frame
        self._requests[request.request_id] = request
        row = self._data["deployments"][parameters["deployment_id"]]
        if key is not None:
            self._data["operations"][key]["result"] = {
                "deployment_id": row["id"],
                "stream": parameters["stream"],
                "request_id": request.request_id,
            }
        self._refresh_streams(row)
        self._save()

    def _before_dispatch(self, request):
        key = self._dispatch_operation
        if (
            (key is None and self._dispatch_feature_event is None)
            or self.closing
            or self.error is not None
        ):
            raise TradingBlocked("causal operation dispatch authority unavailable")
        if key is not None:
            self._data["operations"][key]["result"] = {
                "deployment_id": self.active,
                "stream": request.stream,
                "request_id": request.request_id,
            }
            self._save()  # Manual operation correlation; internal event uses gate intent.
        self.external_features._before_dispatch_actor(request)

    def _refresh_streams(self, row):
        if self.owner is None:
            return
        snapshot = self.owner.snapshot()["state"]
        result = {}
        for stream, item in snapshot["streams"].items():
            pending = next(
                (operation for operation, name, _ in self._pending.values() if name == stream), None
            )
            value = item["feature"]
            uncertain = stream in row["uncertain_streams"]
            if uncertain:
                value = None
            if value is not None:
                value = deepcopy(value)
                value["values"] = dict(value["values"])
                value.pop("stream", None)
                value.pop("instrument", None)
            result[stream] = {
                "deployment_id": row["id"],
                "stream": stream,
                "stream_epoch": item["epoch"],
                "instrument": item["instrument"],
                "state": "ready" if item["health"] == "ready" and not uncertain else "blocked",
                "reason": "inference_outcome_unconfirmed" if uncertain else item["reason"],
                "last_open_ms": item["last_open_ms"],
                "sequence": item["sequence"],
                "pending_operation_id": pending,
                "feature": value,
                "manual_required": item["health"] != "ready" or uncertain,
            }
        row["streams"] = result

    def _suspend(self, *, retire=False):
        row = self._data["deployments"].get(self.active)
        for request_id, (key, stream, _) in tuple(self._pending.items()):
            if row:
                row["uncertain_streams"] = sorted(set(row["uncertain_streams"]) | {stream})
            if key is not None:
                self._data["operations"][key].update(
                    state="unknown",
                    error=failure("PREDICTION_CANCELLED_OR_UNKNOWN"),
                    finished_at_ms=self._now(),
                )
            self._pending.pop(request_id)
            self._requests.pop(request_id, None)
            self.external_features._forgot_actor(request_id)
            self.external_features.events.cancel_actor(request_id)
        if row:
            for item in row["streams"].values():
                item.update(
                    state="manual_required",
                    manual_required=True,
                    feature=None,
                    pending_operation_id=None,
                    reason="owner_retired" if retire else "owner_paused",
                )
            row["state"] = "retired" if retire else "paused"
        self._save()  # Pause intent and unknown peer state precede child cleanup.
        self._cleanup_resources()

    def _cleanup_resources(self):
        resources = (
            self.owner,
            self._unattached_worker,
            self.source,
            *self._quarantined,
            *self._pending_file_bindings({"nn_state"}),
        )
        self.owner, self._unattached_worker, self.source = None, None, None
        self.registry, self.activation, self.active = None, None, None
        self._quarantined = []
        errors, seen = [], set()
        for resource in resources:
            if resource is not None and id(resource) not in seen:
                seen.add(id(resource))
                if type(resource) is StoreLeaseBinding and self._physically_retired(resource):
                    continue  # The owner already confirmed this same original binding.
                close_returned = False
                try:
                    resource.close()
                    close_returned = True
                except BaseException:
                    errors.append(True)
                    if not any(value is resource for value in self._retained_resources):
                        self._retained_resources.append(resource)
                    if not self._physically_retired(resource):
                        self._quarantined.append(resource)
                else:
                    try:
                        probe = getattr(resource, "retirement_status", None)
                        needs_proof = (
                            type(resource) is StoreLeaseBinding
                            or callable(probe)
                            or hasattr(resource, "worker")
                            or any(original is resource for original in self._retained_resources)
                        )
                        confirmed = not needs_proof or self._physically_retired(resource)
                    except BaseException:
                        confirmed = False
                    if not confirmed:
                        errors.append(True)
                        if not any(original is resource for original in self._retained_resources):
                            self._retained_resources.append(resource)
                        self._quarantined.append(resource)
                finally:
                    self._observe_retirement_resource(resource, close_returned=close_returned)
        self._cache_retirement(sealed=False)
        if errors:
            raise StorageError("causal inference resource cleanup is unconfirmed")

    def _observe_retirement_resource(self, resource, *, close_returned):
        originals = self._original_resources
        if originals is None:
            return
        try:
            # Existing cleanup already performed the close. The evidence layer
            # never repeats cleanup or turns a missing ACK into a new success.
            originals.observe_resource(
                resource, close_attempted=True, close_returned=close_returned
            )
        except BaseException:
            self._retirement_capture_failed = True

    def _seal_retirement_admission(self):
        try:
            if self._original_resources is None or self._file_leases is None:
                self._retirement_capture_failed = True
                return
            self._original_resources.seal_admission()
            self._file_leases.seal_admission()
        except BaseException:
            self._retirement_capture_failed = True

    def _cache_retirement(self, *, sealed):
        try:
            if self._original_resources is None or self._retirement_capture_failed:
                return
            if self.store is not self._original_journal_store:
                self._retirement_capture_failed = True
                return
            stores = self._file_leases.snapshot() if self._file_leases is not None else None
            result = self._original_resources.snapshot(
                sealed=sealed,
                stores=stores,
                last_journal_ack=self._retirement_last_ack,
                journal_write_pending=self._retirement_write_pending,
                recording_unknown=self._retirement_recording_unknown,
                actor_error_code=self.error["code"] if self.error else None,
            )
            with self.condition:
                self._retirement_cache = result
        except BaseException:
            # Instrumentation cannot replace a primary cleanup exception. No
            # previous partial record becomes a complete final acknowledgement.
            self._retirement_capture_failed = True

    def retirement_snapshot(self):
        """Internal immutable cache only; never probe an actor-owned resource."""
        with self.condition:
            if self._generation_context is not None and (
                self._retirement_write_pending or self._retirement_recording_unknown
            ):
                return None
            return None if self._retirement_capture_failed else self._retirement_cache

    def actor_join_observation(self, *, timeout_s=0):
        observer = self._actor_join_observer
        if observer is None:
            return None
        return observer.observe(timeout_s, current_thread=self.thread)

    @staticmethod
    def _physically_retired(resource):
        if type(resource) is CommittedInferenceMarketSource:
            try:
                status = resource.retirement_status()
                return (
                    type(status) is SourceReadRetirementSnapshot
                    and status.complete_default_factory is True
                    and status.identity_intact is True
                    and status.physical_confirmed is True
                )
            except BaseException:
                return False
        if type(resource) is StoreLeaseBinding or isinstance(resource, FileInferenceStateStore):
            try:
                status = (
                    resource.snapshot()
                    if type(resource) is StoreLeaseBinding
                    else resource.retirement_status()
                )
                return type(status) is StoreLeaseSnapshot and status.file_lease_confirmed is True
            except BaseException:
                return False
        try:
            worker = getattr(resource, "worker", resource)
            probe = getattr(worker, "retirement_status", None)
            if not callable(probe):
                return False  # A generic failed close is not proof of physical death.
            status = probe()
            store = getattr(resource, "store", None)
            if store is None:
                store_confirmed = True
            elif callable(getattr(store, "retirement_status", None)):
                observation = store.retirement_status()
                store_confirmed = (
                    type(observation) is StoreLeaseSnapshot
                    and observation.file_lease_confirmed is True
                )
            else:
                # Existing injected legacy ports retain their narrow contract;
                # they cannot grant new default-factory restoration authority.
                store_confirmed = store.closed is True
            return (
                status.get("confirmed") is True
                and status.get("worker_alive") is False
                and status.get("host_resources_confirmed", True) is True
                and store_confirmed
            )
        except BaseException:
            return False

    def _pending_file_bindings(self, roles):
        if self._file_leases is None:
            return ()
        return tuple(
            binding
            for binding in self._file_leases.bindings
            if binding.role in roles and not self._physically_retired(binding)
        )

    def _quarantine_file_bindings(self, roles):
        for binding in self._pending_file_bindings(roles):
            if not any(original is binding for original in self._quarantined):
                self._quarantined.append(binding)

    def file_lease_status(self):
        """Foreign callers read cached file evidence only; never an OS/restore ACK."""
        with self.condition:
            return deepcopy(self._file_lease_cache)

    def _close_final_file_stores(self):
        if self._generation_context is not None:
            try:
                # Only this original actor can issue the final membership export.
                # Admission is already fenced by closing/error/final retirement.
                self.external_features._finalize_generation_settlement_actor()
            except BaseException:
                self._retirement_capture_failed = True
        try:
            self.external_features._close_actor()
        except BaseException:
            self._fault("CAUSAL_INFERENCE_CLEANUP_UNCONFIRMED")
        delivery_store = getattr(getattr(self.external_features, "store", None), "store", None)
        if (
            isinstance(delivery_store, FileInferenceStateStore)
            and not self._physically_retired(delivery_store)
            and not any(value is delivery_store for value in self._quarantined)
        ):
            self._quarantined.append(delivery_store)
        # Pause keeps delivery/pins usable; only final actor shutdown reaches
        # this phase. Unknown non-journal leases hold the original journal.
        self._quarantine_file_bindings({"nn_state", "delivery"})
        self._retire_quarantine()
        if self.store is not None:
            try:
                self.store.close()
            except BaseException:
                self._fault("CAUSAL_INFERENCE_CLEANUP_UNCONFIRMED")
                if (
                    isinstance(self.store, FileInferenceStateStore)
                    and not self._physically_retired(self.store)
                    and not any(value is self.store for value in self._quarantined)
                ):
                    self._quarantined.append(self.store)
        self._quarantine_file_bindings({"journal"})
        self._retire_quarantine()

    def _wake_failed_admissions(self):
        self.external_features._fail_waiters_actor()
        with self.condition:
            for ticket in self._tickets.values():
                ticket["error"] = StorageError("causal actor closed before admission ACK")
            self._tickets.clear()
            self.condition.notify_all()

    def _retire_quarantine(self):
        if not self._quarantined:
            return
        self._wake_failed_admissions()
        self._deliver_incidents()
        # One immediate bounded retry handles a transient close ACK failure. A
        # persistent live/unknown resource then waits for explicit close retry;
        # no polling loop or replacement actor can bypass the held journal lock.
        self._cleanup_consumed = self._cleanup_retry
        try:
            self._cleanup_resources()
        except StorageError:
            self._fault("CAUSAL_INFERENCE_CLEANUP_UNCONFIRMED")
        while self._quarantined:
            self._publish()
            self._deliver_incidents()
            with self.condition:
                self.condition.wait_for(lambda: self._cleanup_retry != self._cleanup_consumed)
                self._cleanup_consumed = self._cleanup_retry
            try:
                self._cleanup_resources()
            except StorageError:
                self._fault("CAUSAL_INFERENCE_CLEANUP_UNCONFIRMED")

    def _execute(self, key):
        row = self._data["operations"][key]
        row.update(state="running", started_at_ms=self._now())
        self._save()
        try:
            kind, parameters = row["kind"], row["parameters"]
            if kind == "activate":
                result = self._activate(parameters)
            elif kind == "reset_stream":
                result = self._reset(parameters)
            elif kind == "predict_committed":
                self._predict(parameters, key)
                return
            else:
                deployment = self._deployment(parameters)
                if self.active not in {None, deployment["id"]}:
                    raise TradingBlocked("another causal deployment is active")
                self._suspend(retire=kind == "retire")
                deployment = self._data["deployments"][parameters["deployment_id"]]
                deployment["state"] = "retired" if kind == "retire" else "paused"
                result = {"deployment_id": deployment["id"]}
            self._finish(key, "complete", result=result)
        except StorageError:
            self._fault("CAUSAL_INFERENCE_RECORDING_UNCONFIRMED")
        except Exception:
            if self.owner is None:
                self._cleanup_resources()
                self.activation, self.active = None, None
                for deployment in self._data["deployments"].values():
                    if deployment["state"] == "preparing":
                        deployment.update(state="failed", error=failure("ACTIVATION_FAILED"))
            self._finish(key, "failed", code="INFERENCE_OPERATION_REJECTED")

    def _poll(self):
        if self.owner is None:
            return
        for request_id, (key, stream, frame) in tuple(self._pending.items()):
            if key is None:
                self.external_features.events.poll_actor(request_id)
                continue
            try:
                feature = self.owner.poll(request_id)
                if feature is None:
                    snapshot = self.owner.snapshot()["state"]
                    if request_id in snapshot["pending"]:
                        continue
                    request = self._requests.get(request_id)
                    current = snapshot["streams"].get(stream, {})
                    if (
                        request is not None
                        and current.get("last_request_id") == request_id
                        and current.get("health") == "ready"
                        and current.get("reason") == "no_signal"
                        and current.get("feature") is None
                        and current.get("epoch") == request.stream_epoch
                        and current.get("sequence") == request.sequence
                        and current.get("last_open_ms") == frame.open_time_ms
                        and type(current.get("accepted_at_ms")) is int
                        and request.issued_at_ms
                        <= current["accepted_at_ms"]
                        <= self._now()
                        < request.deadline_ms
                    ):
                        self.external_features._accepted_actor(request_id, None)
                        self._pending.pop(request_id)
                        self._requests.pop(request_id, None)
                        self._refresh_streams(self._data["deployments"][self.active])
                        self._finish(
                            key,
                            "complete",
                            result={
                                "deployment_id": self.active,
                                "stream": stream,
                                "request_id": request_id,
                                "no_signal": True,
                                "feature": None,
                            },
                        )
                        continue
                    self._pending.pop(request_id)
                    self._requests.pop(request_id, None)
                    self.external_features._forgot_actor(request_id)
                    deployment = self._data["deployments"][self.active]
                    deployment["uncertain_streams"] = sorted(
                        set(deployment["uncertain_streams"]) | {stream}
                    )
                    self._refresh_streams(deployment)
                    self._finish(key, "unknown", code="INFERENCE_DEADLINE_OR_OUTCOME_UNCONFIRMED")
                else:
                    self.external_features._accepted_actor(request_id, feature)
                    self._pending.pop(request_id)
                    self._requests.pop(request_id, None)
                    self._refresh_streams(self._data["deployments"][self.active])
                    self._finish(
                        key,
                        "complete",
                        result={
                            "deployment_id": self.active,
                            "stream": stream,
                            "request_id": request_id,
                            "feature": feature_view(feature),
                        },
                    )
            except StorageError:
                self._fault("CAUSAL_INFERENCE_RECORDING_UNCONFIRMED")
                return
            except Exception:
                self._pending.pop(request_id, None)
                self._requests.pop(request_id, None)
                self.external_features._forgot_actor(request_id)
                deployment = self._data["deployments"][self.active]
                deployment["uncertain_streams"] = sorted(
                    set(deployment["uncertain_streams"]) | {stream}
                )
                self._refresh_streams(deployment)
                self._finish(key, "failed", code="INFERENCE_RECEIPT_REJECTED")

    def _receipt_arrived(self):
        """Reader callback only wakes the actor; no model/file/core work here."""
        with self.condition:
            if not self.closed:
                self._receipt_available = True
                self.condition.notify_all()

    def _pause_failed_transport(self):
        if self.owner is None or self.active is None:
            return False
        probe = getattr(self.owner.worker, "transport_status", None)
        status = probe() if callable(probe) else None
        if status is None:
            return False
        if (
            type(status) is not dict
            or set(status) != {"closed", "retired", "failure"}
            or type(status["closed"]) is not bool
            or type(status["retired"]) is not bool
            or status["failure"] is not None
            and (type(status["failure"]) is not str or len(status["failure"]) > 384)
        ):
            raise StorageError("causal transport observation is unconfirmed")
        if status["closed"] or not status["retired"] and status["failure"] is None:
            return False
        row = self._data["deployments"][self.active]
        if row["state"] == "paused" and (row["error"] or {}).get("code") == (
            "CAUSAL_INFERENCE_TRANSPORT_UNAVAILABLE"
        ):
            return status["retired"]
        row.update(state="paused", error=failure("CAUSAL_INFERENCE_TRANSPORT_UNAVAILABLE"))
        # Block fresh feature admission before IO. Previously admitted pins and
        # independently valid complete receipts keep their original evidence.
        self.external_features._sync_actor()
        self._save()  # Durable pause intent precedes cleanup of the failed port.
        self._incidents.add("CAUSAL_INFERENCE_TRANSPORT_UNAVAILABLE")
        return status["retired"]

    def _enqueue_feature_command(self, ticket):
        with self.condition:
            if (
                not self.ready
                or self.closing
                or self.closed
                or self.error is not None
                or len(self._feature_commands) >= 32
            ):
                return False
            self._feature_commands.append(ticket)
            self.condition.notify_all()
            return True

    def read_feature_settlement(self, ref, *, host_token):
        return self.external_features.read_feature_settlement(ref, host_token=host_token)

    def ack_feature_settlement(self, ref, ack, *, host_token):
        return self.external_features.ack_feature_settlement(ref, ack, host_token=host_token)

    def confirm_feature_settlement_ack(self, ref, ack, *, host_token):
        return self.external_features.confirm_feature_settlement_ack(
            ref, ack, host_token=host_token
        )

    def seal_feature_settlement(self, *, host_token):
        return self.external_features.seal_feature_settlement(host_token=host_token)

    def generation_settlement_snapshot(self):
        return self.external_features.generation_settlement_snapshot()

    def carry_acceptance_snapshot(self):
        return self.external_features.carry_acceptance_snapshot()

    def manual_startup_certificate(self):
        return self._manual_startup_certificate

    def _enqueue_feature_event(self, event):
        if not self.condition.acquire(blocking=False):
            return False
        try:
            if (
                not self.ready
                or self.closing
                or self.closed
                or self.error is not None
                or len(self._feature_events) >= 128
            ):
                return False
            self._feature_events.append(event)
            self.condition.notify_all()
            return True
        finally:
            self.condition.release()

    def _is_committed_no_signal(self, request, state):
        if request is None:
            return False
        current = state["streams"].get(request.stream, {})
        return (
            current.get("last_request_id") == request.request_id
            and current.get("health") == "ready"
            and current.get("reason") == "no_signal"
            and current.get("feature") is None
            and current.get("epoch") == request.stream_epoch
            and current.get("sequence") == request.sequence
            and current.get("last_open_ms") == request.frame.open_time_ms
            and type(current.get("accepted_at_ms")) is int
            and request.issued_at_ms
            <= current["accepted_at_ms"]
            <= self._now()
            < request.deadline_ms
            and request.core_epoch == self.core_epoch
            and request.deployment_epoch == self.active
        )

    def _loop(self):
        try:
            self._initialize()
            self._deliver_storage_warnings()
            while not self.closing and self.error is None:
                ticket = key = feature_ticket = feature_event = None
                with self.condition:
                    if self._admissions:
                        ticket = self._admissions.popleft()
                    elif self._work:
                        key = self._work.popleft()
                    elif self._receipt_available:
                        self._receipt_available = False
                    elif self._feature_commands:
                        feature_ticket = self._feature_commands.popleft()
                    elif self._feature_events and len(self._pending) < 8:
                        feature_event = self._feature_events.popleft()
                    elif not self._pending:
                        self.condition.wait()
                        continue
                    else:
                        self.condition.wait(timeout=0.05)
                if self.closing:
                    break
                if ticket is not None:
                    self._admit(ticket)
                elif key is not None:
                    self._execute(key)
                elif feature_ticket is not None:
                    self.external_features._run_command_actor(feature_ticket)
                elif feature_event is not None:
                    self.external_features.events.run_actor(feature_event)
                failed_transport = self._pause_failed_transport()
                self._poll()
                if not failed_transport and self.error is None:
                    failed_transport = self._pause_failed_transport()
                    if failed_transport:
                        # Terminal handoff may become visible during the first
                        # pass. Consume its already complete cached receipts
                        # before the pause marks any remaining work unknown.
                        self._poll()
                if failed_transport and self.owner is not None and self.error is None:
                    self._suspend()
                self._deliver_incidents()
                self._deliver_storage_warnings()
        except BaseException:
            self._fault("CAUSAL_INFERENCE_UNAVAILABLE")
        finally:
            try:
                self._seal_retirement_admission()
                try:
                    if self._data is not None and self.error is None:
                        for row in self._data["operations"].values():
                            if row["state"] not in TERMINAL:
                                row.update(
                                    state="unknown",
                                    error=failure("OWNER_CLOSED_BEFORE_COMPLETION"),
                                    finished_at_ms=self._now(),
                                )
                        self._suspend()
                    else:
                        self._cleanup_resources()
                except Exception:
                    self._fault("CAUSAL_INFERENCE_CLEANUP_UNCONFIRMED")
                finally:
                    self._retire_quarantine()
                    self._close_final_file_stores()
                    self.closed, self.ready = True, False
                    self._wake_failed_admissions()
                    self._publish()
                    self._deliver_incidents()
                    self._deliver_storage_warnings()
            finally:
                self._cache_retirement(sealed=True)

    def close(self):
        with self.condition:
            self.closing = True
            self._cleanup_retry += 1
            self.condition.notify_all()
        if (
            self.thread is threading.current_thread()
            or self._original_actor_thread is threading.current_thread()
        ):
            raise StorageError("causal actor cleanup cannot be confirmed from its own callback")
        if self._actor_join_observer is not None:
            observation = self.actor_join_observation(timeout_s=self.close_timeout_s)
            if not observation.original_thread_dead:
                raise StorageError("causal actor cleanup deadline exceeded")
            if not observation.identity_intact:
                raise StorageError("causal original actor identity is unconfirmed")
        elif self.thread is not None:
            self.thread.join(timeout=self.close_timeout_s)
            if self.thread.is_alive():
                raise StorageError("causal actor cleanup deadline exceeded")
        else:
            self.closed, self.ready = True, False
            self._publish()
        if self.error:
            raise StorageError("causal actor recording or cleanup is unconfirmed")
        if not self.closed:
            raise StorageError("causal actor final cleanup acknowledgement is unconfirmed")
        return {
            **FLAGS,
            "closed": True,
            "durable": True,
            "worker_alive": False,
            "actor_alive": False,
        }
