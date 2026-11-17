"""Past startup observations, never a live worker/file-close capability.

Default startup builds no source/owner/worker. A later accepted control commits
the service journal before owner construction; only worker.submit starts a child.
Cold use also requires exact unchanged heads and current managed-parent exclusion.
"""

import ast
import threading
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path

from omega.domain.errors import ValidationError
from omega.domain.inference import InferenceCommitAck, checksum, digest, identity

POLICY_ID = "omega.causal.manual-startup.no-child-before-control.v2"


def manual_startup_policy_sha256():
    """Bind the default causal code closure, independent of paths/line numbers/OS.

    Conservative version matching includes orchestration and helper modules so
    changing a later caller cannot silently retain the old startup policy hash.
    No model/plugin Python is imported or inspected here.
    """
    package = Path(__file__).resolve().parents[1]
    paths = sorted((package / "runtime").glob("causal*.py"))
    paths.extend(
        package / name
        for name in (
            "runtime/control.py",
            "domain/inference.py",
            "domain/external_features.py",
            "domain/models.py",
        )
    )
    if not 1 <= len(paths) <= 128:
        raise ValidationError("startup policy code closure exceeds bounds")
    shapes, total = {}, 0
    for path in paths:
        if path.stat().st_size > 4 * 1024 * 1024:
            raise ValidationError("startup policy component exceeds bounds")
        raw = path.read_bytes()
        total += len(raw)
        if total > 8 * 1024 * 1024:
            raise ValidationError("startup policy code closure exceeds bounds")
        tree = ast.parse(raw.decode("utf-8"))
        shapes[path.relative_to(package).as_posix()] = sha256(
            ast.dump(tree, include_attributes=False).encode("utf-8")
        ).hexdigest()
    return digest({"policy_id": POLICY_ID, "components": shapes})


@dataclass(frozen=True, slots=True)
class ManualStartupCertificate:
    core_epoch: str
    generation: int
    actor_token: str
    owner_thread_token: str
    context_sha256: str
    policy_sha256: str
    provider_token: str
    store_scope_token: str
    store_binding_token: str
    settlement_sha256: str
    journal_revision: int
    journal_sha256: str
    delivery_revision: int
    delivery_sha256: str
    schema_version: int = 2
    policy_id: str = POLICY_ID
    default_factories: bool = True
    streams_manual: bool = True
    owner_constructed: bool = False
    source_constructed: bool = False
    inference_child_started: bool = False
    past_observation_only: bool = True
    physical_close_claimed: bool = False
    all_kernel: bool = False

    def __post_init__(self):
        for value in (
            self.core_epoch,
            self.actor_token,
            self.owner_thread_token,
            self.provider_token,
            self.store_scope_token,
            self.store_binding_token,
        ):
            identity(value)
        for value in (
            self.context_sha256,
            self.policy_sha256,
            self.journal_sha256,
            self.delivery_sha256,
            self.settlement_sha256,
        ):
            checksum(value)
        for value in (self.generation, self.journal_revision, self.delivery_revision):
            if type(value) is not int or not 0 <= value < 2**63:
                raise ValidationError("bounded startup observation revision required")
        if (
            type(self.schema_version) is not int
            or self.schema_version != 2
            or self.policy_id != POLICY_ID
        ):
            raise ValidationError("unsupported manual startup policy")
        if any(
            value is not True
            for value in (
                self.default_factories,
                self.streams_manual,
                self.past_observation_only,
            )
        ) or any(
            value is not False
            for value in (
                self.owner_constructed,
                self.source_constructed,
                self.inference_child_started,
                self.physical_close_claimed,
                self.all_kernel,
            )
        ):
            raise ValidationError("startup data cannot grant physical or inference authority")

    def document(self):
        body = asdict(self)
        return {**body, "certificate_sha256": digest(body)}

    @property
    def sha256(self):
        return self.document()["certificate_sha256"]

    @classmethod
    def parse(cls, document):
        if type(document) is not dict or set(document) != set(cls.__dataclass_fields__) | {
            "certificate_sha256"
        }:
            raise ValidationError("exact manual startup observation required")
        result = cls(**{name: document[name] for name in cls.__dataclass_fields__})
        if result.document() != document:
            raise ValidationError("manual startup observation digest differs")
        return result


def capture_manual_startup_certificate(service):
    """Original actor-only default observation; unavailable data returns None."""
    from omega.runtime.causal_feature_delivery import CausalFeatureDeliveryProvider
    from omega.runtime.causal_feature_delivery_store import CausalFeatureDeliveryStore
    from omega.runtime.causal_generation_outcomes import (
        GenerationContext,
        GenerationSettlementSnapshot,
    )
    from omega.runtime.causal_inference_service import CausalInferenceService
    from omega.runtime.causal_market_source import CommittedInferenceMarketSource
    from omega.runtime.causal_nn import CausalNNFeatureOwner, FileInferenceStateStore
    from omega.runtime.causal_nn_worker import CausalInferenceProcessWorker

    if (
        type(service) is not CausalInferenceService
        or service._original_actor_thread is not threading.current_thread()
    ):
        return None
    context = service._generation_context
    if type(context) is not GenerationContext:
        return None
    expected = (
        ("store", FileInferenceStateStore),
        ("owner", CausalNNFeatureOwner),
        ("worker", CausalInferenceProcessWorker),
        ("source", CommittedInferenceMarketSource),
    )
    if any(
        getattr(service, "_default_" + name + "_factory") is not True
        or getattr(service, name + "_factory") is not factory
        for name, factory in expected
    ):
        return None
    if (
        any(
            getattr(service, name) is not None
            for name in (
                "owner",
                "source",
                "activation",
                "active",
                "registry",
                "_unattached_worker",
            )
        )
        or service._pending
        or service._retained_resources
        or service._quarantined
        or service.error is not None
        or service.closing
        or service.closed
        or type(service.external_features) is not CausalFeatureDeliveryProvider
        or type(service.store) is not FileInferenceStateStore
    ):
        return None
    provider = service.external_features
    store = provider.store
    if (
        type(store) is not CausalFeatureDeliveryStore
        or type(store.store) is not FileInferenceStateStore
    ):
        return None
    retirement = service.retirement_snapshot()
    membership = provider.generation_settlement_snapshot()
    journal = None if retirement is None else retirement.last_journal_ack
    delivery = store._settlement_last_exact_ack
    if (
        type(membership) is not GenerationSettlementSnapshot
        or membership.context_sha256 != context.binding_sha256
        or (membership.core_epoch, membership.generation, membership.actor_token)
        != (context.core_epoch, context.generation, context.actor_token)
        or membership.last_exact_ack is not delivery
        or membership.current_write_confirmed is not True
        or membership.recording_unknown
        or membership.capture_failed
        or membership.sealed is not False
        or membership.outcomes
        or type(journal) is not InferenceCommitAck
        or type(delivery) is not InferenceCommitAck
        or service.store._last_ack is not journal
        or store.store._last_ack is not delivery
        or journal.durable is not True
        or delivery.durable is not True
        or retirement.identity_intact is not True
        or retirement.factory_coverage_complete is not True
        or retirement.capture_failed
        or retirement.recording_unknown
        or retirement.journal_write_pending
        or store._settlement_current_write_confirmed is not True
        or store._settlement_recording_unknown
        or store._settlement_write_pending
        or service.store.failed
        or store.store.failed
        or service._stored_revision != journal.revision
        or service._data["revision"] != journal.revision
        or digest(service._data) != journal.snapshot_sha256
        or store.revision != delivery.revision
        or digest(store.data) != delivery.snapshot_sha256
    ):
        return None
    if any(
        row["state"] not in {"manual_required", "retired", "failed"}
        or any(
            stream["manual_required"] is not True or stream["pending_operation_id"] is not None
            for stream in row["streams"].values()
        )
        for row in service._data["deployments"].values()
    ):
        return None
    try:
        policy = manual_startup_policy_sha256()
    except (OSError, TypeError, SyntaxError, UnicodeError, ValidationError):
        return None  # Optional cold adoption cannot interfere with normal inference.
    return ManualStartupCertificate(
        core_epoch=context.core_epoch,
        generation=context.generation,
        actor_token=context.actor_token,
        owner_thread_token=service._actor_thread_token,
        context_sha256=context.binding_sha256,
        policy_sha256=policy,
        provider_token=membership.provider_token,
        store_scope_token=membership.store_scope_token,
        store_binding_token=membership.store_binding_token,
        settlement_sha256=membership.sha256,
        journal_revision=journal.revision,
        journal_sha256=journal.snapshot_sha256,
        delivery_revision=delivery.revision,
        delivery_sha256=delivery.snapshot_sha256,
    )
