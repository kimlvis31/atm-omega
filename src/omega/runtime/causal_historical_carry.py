"""Immutable historical obligations and bounded default-file readers.

These values identify bytes, never physical-close or live writer authority.
The host validates original retirement before passing an install request.
"""

from dataclasses import asdict, dataclass
from pathlib import Path

from omega.domain.errors import StorageError, ValidationError
from omega.domain.external_features import ExternalFeaturePin, ExternalFeatureSnapshot
from omega.domain.inference import (
    CAUSAL_SCOPE,
    InferenceCommitAck,
    checksum,
    decoded,
    digest,
    encoded,
    identity,
)
from omega.research.artifacts import validate_artifact_path
from omega.runtime.causal_capacity import EvidenceBudgetLimits
from omega.runtime.causal_generation_outcomes import (
    GenerationContext,
    GenerationSettlementSnapshot,
    covered_rejected_outcome,
)
from omega.runtime.causal_inference_registry import validate_journal
from omega.runtime.causal_nn_proof import read_document

MAX_CARRY_BYTES = 1_048_576


def _integer(value):
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValidationError("bounded native carry integer required")


@dataclass(frozen=True, slots=True)
class CarryManifest:
    origin_core_epoch: str
    origin_generation: int
    origin_actor_token: str
    origin_provider_token: str
    origin_store_scope_token: str
    origin_store_binding_token: str
    physical_proof_sha256: str
    journal_revision: int
    journal_sha256: str
    settlement_sha256: str
    registry_json: str
    provenance_json: str
    pins: tuple[ExternalFeaturePin, ...]
    reservations: tuple[tuple[str, int], ...]
    recording_unknown: bool
    unresolved_outcomes: bool

    def __post_init__(self):
        for name in (
            "origin_core_epoch",
            "origin_actor_token",
            "origin_provider_token",
            "origin_store_scope_token",
            "origin_store_binding_token",
        ):
            identity(getattr(self, name))
        for value in (self.origin_generation, self.journal_revision):
            _integer(value)
        for value in (self.physical_proof_sha256, self.journal_sha256, self.settlement_sha256):
            checksum(value)
        registry, provenance = decoded(self.registry_json), decoded(self.provenance_json)
        if (
            type(registry) is not dict
            or type(provenance) is not dict
            or encoded(registry) != self.registry_json
            or encoded(provenance) != self.provenance_json
            or set(provenance) != {"snapshots", "pin_records", "reservations"}
            or type(self.pins) is not tuple
            or type(self.reservations) is not tuple
            or any(type(pin) is not ExternalFeaturePin for pin in self.pins)
            or tuple(sorted(self.pins, key=lambda pin: pin.pin_id)) != self.pins
            or len({pin.pin_id for pin in self.pins}) != len(self.pins)
            or tuple(sorted(self.reservations)) != self.reservations
            or any(
                type(flag) is not bool
                for flag in (self.recording_unknown, self.unresolved_outcomes)
            )
        ):
            raise ValidationError("canonical bounded carry membership required")
        if tuple(sorted(registry.get("reservations", {}).items())) != self.reservations:
            raise ValidationError("carry reservations differ from original registry")
        _registry(registry, _maximum_limits())
        if any(type(provenance[name]) is not list for name in provenance):
            raise ValidationError("bounded carry provenance lists required")
        encoded(self.document(), maximum=MAX_CARRY_BYTES)

    @property
    def delivery_revision(self):
        return decoded(self.registry_json)["revision"]

    @property
    def delivery_sha256(self):
        return digest(decoded(self.registry_json))

    @property
    def charged_records(self):
        return decoded(self.registry_json)["records"]

    @property
    def charged_bytes(self):
        return decoded(self.registry_json)["bytes"]

    @property
    def has_unknown_reservations(self):
        return any(
            row["state"] == "unknown" for row in decoded(self.provenance_json)["reservations"]
        )

    def document(self):
        result = asdict(self)
        result["registry"] = decoded(result.pop("registry_json"))
        result["provenance"] = decoded(result.pop("provenance_json"))
        result["pins"] = [pin.document() for pin in self.pins]
        result["reservations"] = [list(row) for row in self.reservations]
        return result

    @property
    def sha256(self):
        return digest(self.document())

    @classmethod
    def parse(cls, document):
        if type(document) is not dict:
            raise ValidationError("carry manifest object required")
        try:
            values = dict(document)
            values["registry_json"] = encoded(values.pop("registry"))
            values["provenance_json"] = encoded(values.pop("provenance"))
            values["pins"] = tuple(ExternalFeaturePin.parse(pin) for pin in values["pins"])
            values["reservations"] = tuple(tuple(row) for row in values["reservations"])
            result = cls(**values)
            if encoded(result.document()) != encoded(document):
                raise ValidationError("carry manifest is not canonical")
            return result
        except (KeyError, TypeError, ValueError):
            raise ValidationError("invalid carry manifest") from None


@dataclass(frozen=True, slots=True)
class CarryInstallRequest:
    manifest: CarryManifest
    new_core_epoch: str
    new_generation: int
    new_actor_token: str

    def __post_init__(self):
        if type(self.manifest) is not CarryManifest:
            raise ValidationError("exact carry manifest required")
        identity(self.new_core_epoch)
        identity(self.new_actor_token)
        _integer(self.new_generation)
        if (
            self.new_generation != self.manifest.origin_generation + 1
            or self.new_actor_token == self.manifest.origin_actor_token
            or self.manifest.recording_unknown
            or self.manifest.unresolved_outcomes
        ):
            raise ValidationError(
                "carry requires a new reserved generation and settled original writer"
            )

    def document(self):
        return {
            "manifest": self.manifest.document(),
            "new_core_epoch": self.new_core_epoch,
            "new_generation": self.new_generation,
            "new_actor_token": self.new_actor_token,
        }


@dataclass(frozen=True, slots=True)
class CarryAcceptedAck:
    manifest_sha256: str
    new_core_epoch: str
    new_generation: int
    new_actor_token: str
    store_scope_token: str
    store_binding_token: str
    observed_revision: int
    observed_sha256: str
    commit_ack: InferenceCommitAck

    def __post_init__(self):
        checksum(self.manifest_sha256)
        checksum(self.observed_sha256)
        for value in (
            self.new_core_epoch,
            self.new_actor_token,
            self.store_scope_token,
            self.store_binding_token,
        ):
            identity(value)
        for value in (self.new_generation, self.observed_revision):
            _integer(value)
        if (
            type(self.commit_ack) is not InferenceCommitAck
            or self.commit_ack.durable is not True
            or self.commit_ack.revision != self.observed_revision + 1
        ):
            raise ValidationError("exact new carry writer ACK required")

    def document(self):
        return asdict(self)

    @property
    def sha256(self):
        return digest(self.document())


def _head(root, name, *, expected_revision=None, expected_sha256=None):
    """Read known default envelopes/anchors without acquiring any writer lease."""
    path = Path(root) / "state" / name
    envelope = read_document(path, maximum=2 * MAX_CARRY_BYTES)
    anchor = read_document(path.with_suffix(path.suffix + ".origin.json"))
    if (
        type(envelope) is not dict
        or set(envelope) != {"schema_version", "revision", "snapshot_sha256", "snapshot"}
        or type(envelope["schema_version"]) is not int
        or envelope["schema_version"] != 1
        or type(envelope["revision"]) is not int
        or envelope["revision"] < 0
        or type(envelope["snapshot"]) is not dict
        or type(envelope["snapshot"].get("revision")) is not int
        or envelope["snapshot"].get("revision") != envelope["revision"]
        or digest(envelope["snapshot"]) != envelope["snapshot_sha256"]
        or type(anchor) is not dict
        or set(anchor) != {"data", "sha256"}
        or type(anchor["data"]) is not dict
        or set(anchor["data"])
        != {"schema_version", "file_id", "initial_revision", "initial_sha256"}
        or type(anchor["data"]["schema_version"]) is not int
        or anchor["data"]["schema_version"] != 1
        or anchor["data"]["file_id"] != digest(path.name)
        or type(anchor["data"]["initial_revision"]) is not int
        or not 0 <= anchor["data"]["initial_revision"] <= envelope["revision"]
        or anchor["sha256"] != digest(anchor["data"])
        or expected_revision is not None
        and envelope["revision"] != expected_revision
        or expected_sha256 is not None
        and envelope["snapshot_sha256"] != expected_sha256
    ):
        raise StorageError("original carry registry head or anchor differs")
    checksum(anchor["data"]["initial_sha256"])
    if (
        envelope["revision"] == anchor["data"]["initial_revision"]
        and envelope["snapshot_sha256"] != anchor["data"]["initial_sha256"]
    ):
        raise StorageError("original carry registry origin differs")
    return envelope["snapshot"]


def _maximum_limits():
    return EvidenceBudgetLimits(max_bytes=8 * 1024**3, max_records=1000000, max_pins=65536)


def read_default_carry_heads(root):
    """Fixed default envelopes, data only: no writer, ACK or resource capability."""
    journal = _head(root, "causal-inference.json")
    validate_journal(journal)
    delivery = _head(root, "causal-feature-delivery.json")
    _registry(delivery, _maximum_limits())
    return tuple(
        {
            "schema_version": 1,
            "revision": document["revision"],
            "snapshot_sha256": digest(document),
            "snapshot": document,
        }
        for document in (journal, delivery)
    )


def _registry(document, limits):
    if (
        type(document) is not dict
        or set(document)
        != {
            "scope",
            "schema_version",
            "revision",
            "records",
            "bytes",
            "latest",
            "pins",
            "reservations",
        }
        or document["scope"] != CAUSAL_SCOPE
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
    ):
        raise StorageError("invalid original delivery registry")
    for name in ("revision", "records", "bytes"):
        _integer(document[name])
    if (
        any(type(document[name]) is not dict for name in ("latest", "pins", "reservations"))
        or len(document["latest"]) > 128
        or len(document["pins"]) > limits.max_pins
        or len(document["reservations"]) > 64
        or document["bytes"] > limits.max_bytes + limits.max_pins * 2048
        or document["records"] > limits.max_records + limits.max_pins
    ):
        raise StorageError("original delivery membership exceeds capacity")
    for key, value in document["latest"].items():
        checksum(key)
        checksum(value)
    for key, value in document["pins"].items():
        checksum(key)
        if type(value) is not dict or set(value) != {"snapshot", "proof_sha256"}:
            raise StorageError("invalid original pin membership")
        checksum(value["snapshot"])
        checksum(value["proof_sha256"])
    for key, size in document["reservations"].items():
        if (
            type(key) is not str
            or len(key) > 256
            or type(size) is not int
            or not 0 <= size <= 2097152
        ):
            raise StorageError("invalid original evidence reservation")
    if (
        sum(document["reservations"].values()) > document["bytes"]
        or len(document["reservations"]) > document["records"]
    ):
        raise StorageError("original registry omitted charged reservations")


def _provenance(root, registry):
    # Reuse the existing arithmetic/input/receipt validator without importing the
    # delivery store at module load (the store itself imports these leaf types).
    from omega.runtime.causal_feature_delivery_store import validate_actual_evidence

    root = Path(root)
    directory = root / "artifacts" / "causal-feature-delivery"
    snapshots, pin_records, actual_pins = {}, {}, []

    def read_snapshot(key):
        if key in snapshots:
            return
        checksum(key)
        record = read_document(directory / "snapshots" / (key + ".json"))
        if (
            type(record) is not dict
            or set(record)
            != {"scope", "snapshot", "input_ref", "receipt", "output_bindings", "record_sha256"}
            or record["scope"] != "causal_external_snapshot_record"
            or digest({name: value for name, value in record.items() if name != "record_sha256"})
            != record["record_sha256"]
        ):
            raise StorageError("original carry snapshot record differs")
        snapshot = ExternalFeatureSnapshot.parse(record["snapshot"])
        ref = Path(record["input_ref"])
        if (
            ref.is_absolute()
            or ".." in ref.parts
            or ref.as_posix() != record["input_ref"]
            or ":" in record["input_ref"]
            or "\\" in record["input_ref"]
        ):
            raise StorageError("original carry input reference escaped its root")
        evidence = read_document(validate_artifact_path(root / ref))
        if snapshot.snapshot_sha256 != key:
            raise StorageError("original carry snapshot identity differs")
        validate_actual_evidence(snapshot, evidence, record["receipt"], record["output_bindings"])
        snapshots[key] = {
            "snapshot_sha256": key,
            "binding_sha256": snapshot.binding.binding_sha256,
            "provider_id": snapshot.binding.provider_id,
            "record_sha256": record["record_sha256"],
            "input_ref": ref.as_posix(),
            "input_document_sha256": digest(evidence),
            "receipt_sha256": digest(record["receipt"]),
            "request_id": snapshot.request_id,
        }

    def read_pin(key, *, member):
        if key in pin_records:
            return pin_records[key]
        checksum(key)
        pin = ExternalFeaturePin.parse(read_document(directory / "pins" / (key + ".json")))
        if pin.pin_id != key:
            raise StorageError("original carry pin identity differs")
        read_snapshot(pin.snapshot_sha256)
        snapshot = snapshots[pin.snapshot_sha256]
        if (
            pin.binding_sha256 != snapshot["binding_sha256"]
            or pin.provider_id != snapshot["provider_id"]
        ):
            raise StorageError("original carry pin differs from checked snapshot binding")
        consumed_path = validate_artifact_path(
            directory / "consumed" / (key + ".json"), allow_missing=True
        )
        consumed = None
        if consumed_path.exists():
            consumed = read_document(consumed_path)
            if (
                type(consumed) is not dict
                or set(consumed) != {"scope", "pin_id", "consumer_receipt_sha256", "record_sha256"}
                or consumed["scope"] != "consumed_external_feature_pin"
                or consumed["pin_id"] != key
                or consumed["record_sha256"]
                != digest(
                    {name: value for name, value in consumed.items() if name != "record_sha256"}
                )
            ):
                raise StorageError("original consumed pin evidence conflicts")
            checksum(consumed["consumer_receipt_sha256"])
        row = {"pin": pin.document(), "membership_present": member, "consumed": consumed}
        pin_records[key] = row
        if member and consumed is None:
            actual_pins.append(pin)
        return row

    for binding, key in sorted(registry["latest"].items()):
        read_snapshot(key)
        if snapshots[key]["binding_sha256"] != binding:
            raise StorageError("original latest pointer differs from checked snapshot binding")
    for key, membership in sorted(registry["pins"].items()):
        row = read_pin(key, member=True)
        pin = ExternalFeaturePin.parse(row["pin"])
        if (
            digest(pin.document()) != membership["proof_sha256"]
            or pin.snapshot_sha256 != membership["snapshot"]
        ):
            raise StorageError("original full pin membership differs")
    reservations = []
    for key, size in sorted(registry["reservations"].items()):
        kind, separator, record_id = key.partition(":")
        row = {
            "key": key,
            "size": size,
            "kind": "unknown",
            "state": "unknown",
            "evidence_sha256": None,
        }
        if separator and kind in {"input", "snapshot", "pin", "release"}:
            checksum(record_id)
            row["kind"] = kind
            if kind == "snapshot":
                path = validate_artifact_path(
                    directory / "snapshots" / (record_id + ".json"), allow_missing=True
                )
                if path.exists():
                    read_snapshot(record_id)
                    row.update(
                        state="present", evidence_sha256=snapshots[record_id]["record_sha256"]
                    )
            elif kind in {"pin", "release"}:
                path = validate_artifact_path(
                    directory / "pins" / (record_id + ".json"), allow_missing=True
                )
                if path.exists():
                    pin = read_pin(record_id, member=record_id in registry["pins"])
                    evidence = pin["pin"] if kind == "pin" else pin["consumed"]
                    if evidence is not None:
                        row.update(state="present", evidence_sha256=digest(evidence))
            else:
                evidence = next(
                    (value for value in snapshots.values() if value["request_id"] == record_id),
                    None,
                )
                if evidence is not None:
                    row.update(state="present", evidence_sha256=evidence["input_document_sha256"])
        reservations.append(row)
    provenance = {
        "snapshots": [snapshots[key] for key in sorted(snapshots)],
        "pin_records": [pin_records[key] for key in sorted(pin_records)],
        "reservations": reservations,
    }
    return provenance, tuple(sorted(actual_pins, key=lambda pin: pin.pin_id))


def build_carry_manifest(
    root,
    *,
    context,
    physical_proof_sha256,
    journal_ack,
    settlement,
    limits=None,
    terminal_coverage=(),
):
    limits = EvidenceBudgetLimits() if limits is None else limits
    if (
        type(limits) is not EvidenceBudgetLimits
        or type(context) is not GenerationContext
        or type(journal_ack) is not InferenceCommitAck
        or journal_ack.durable is not True
        or type(settlement) is not GenerationSettlementSnapshot
        or not settlement.sealed
        or settlement.context_sha256 != context.binding_sha256
        or (settlement.core_epoch, settlement.generation, settlement.actor_token)
        != (context.core_epoch, context.generation, context.actor_token)
        or settlement.recording_unknown
        or settlement.capture_failed
        or not settlement.current_write_confirmed
        or type(settlement.last_exact_ack) is not InferenceCommitAck
        or settlement.last_exact_ack.durable is not True
        or settlement.last_exact_ack.revision != settlement.head_revision
        or settlement.last_exact_ack.snapshot_sha256 != settlement.head_sha256
    ):
        raise StorageError("exact settled original carry evidence required")
    checksum(physical_proof_sha256)
    journal = _head(
        root,
        "causal-inference.json",
        expected_revision=journal_ack.revision,
        expected_sha256=journal_ack.snapshot_sha256,
    )
    validate_journal(journal)
    registry = _head(
        root,
        "causal-feature-delivery.json",
        expected_revision=settlement.head_revision,
        expected_sha256=settlement.head_sha256,
    )
    _registry(registry, limits)
    provenance, pins = _provenance(root, registry)
    if (
        pins != settlement.pins
        or tuple(sorted(registry["reservations"].items())) != settlement.reservations
    ):
        raise StorageError("actual historical obligations differ from sealed original export")
    manifest = CarryManifest(
        context.core_epoch,
        context.generation,
        context.actor_token,
        settlement.provider_token,
        settlement.store_scope_token,
        settlement.store_binding_token,
        physical_proof_sha256,
        journal_ack.revision,
        journal_ack.snapshot_sha256,
        settlement.sha256,
        encoded(registry),
        encoded(provenance),
        pins,
        settlement.reservations,
        settlement.recording_unknown,
        any(
            row.recording_unknown
            or row.capture_failed
            or not (
                row.phase == "complete"
                or covered_rejected_outcome(context, settlement, row, terminal_coverage)
            )
            for row in settlement.outcomes
        ),
    )
    if len(encoded(manifest.document()).encode("utf-8")) > min(MAX_CARRY_BYTES, limits.max_bytes):
        raise StorageError("historical carry manifest capacity exhausted")
    return manifest


def verify_carry_install(root, request, registry_json, *, limits=None):
    """Owner holds its one new file lease; this gate performs no save or refund."""
    limits = EvidenceBudgetLimits() if limits is None else limits
    if type(request) is not CarryInstallRequest or type(limits) is not EvidenceBudgetLimits:
        raise ValidationError("exact historical carry install request required")
    _verify_manifest_bytes(root, request.manifest, registry_json, limits)


def _verify_manifest_bytes(root, manifest, registry_json, limits):
    registry = _head(
        root,
        "causal-feature-delivery.json",
        expected_revision=manifest.delivery_revision,
        expected_sha256=manifest.delivery_sha256,
    )
    _registry(registry, limits)
    if encoded(registry) != registry_json or registry_json != manifest.registry_json:
        raise StorageError("historical carry expected delivery head changed before first save")
    journal = _head(
        root,
        "causal-inference.json",
        expected_revision=manifest.journal_revision,
        expected_sha256=manifest.journal_sha256,
    )
    validate_journal(journal)
    provenance, pins = _provenance(root, registry)
    if (
        encoded(provenance) != manifest.provenance_json
        or pins != manifest.pins
        or tuple(sorted(registry["reservations"].items())) != manifest.reservations
        or len(encoded(manifest.document()).encode("utf-8"))
        > min(MAX_CARRY_BYTES, limits.max_bytes)
    ):
        raise StorageError("historical carry provenance changed before first save")


def verify_clean_closed_carry_manifest(root, manifest, *, limits):
    """Verify historical clean-close data; preserve its exact original observation.

    Root exclusion and archived positive original close remain Host predicates.
    This read creates neither an old ACK nor a current writer/resource capability.
    """
    if type(manifest) is not CarryManifest or type(limits) is not EvidenceBudgetLimits:
        raise ValidationError("exact closed carry manifest and limits required")
    if manifest.recording_unknown or manifest.unresolved_outcomes:
        raise StorageError("closed carry retained unresolved original recording/effects")
    _verify_manifest_bytes(root, manifest, manifest.registry_json, limits)
    return manifest


def build_cold_carry_manifest(root, certificate, *, exclusion_sha256, limits):
    """Reconstruct only certified prior-candidate startup DATA from actual bytes.

    The exclusion SHA identifies a NEW current-owner managed-parent observation,
    never an original actor close ACK. The Host must independently validate Root,
    policy, archived observations and phase before any factory or install call.
    No parsed past value is promoted to GenerationContext/InferenceCommitAck.
    """
    from omega.runtime.causal_startup_policy import ManualStartupCertificate

    if (
        type(certificate) is not ManualStartupCertificate
        or type(limits) is not EvidenceBudgetLimits
    ):
        raise ValidationError("exact cold startup observation and limits required")
    checksum(exclusion_sha256)
    journal = _head(
        root,
        "causal-inference.json",
        expected_revision=certificate.journal_revision,
        expected_sha256=certificate.journal_sha256,
    )
    validate_journal(journal)
    if journal["core_epoch"] != certificate.core_epoch:
        raise StorageError("cold journal differs from certified prior candidate")
    registry = _head(
        root,
        "causal-feature-delivery.json",
        expected_revision=certificate.delivery_revision,
        expected_sha256=certificate.delivery_sha256,
    )
    _registry(registry, limits)
    provenance, pins = _provenance(root, registry)
    reservations = tuple(sorted(registry["reservations"].items()))
    historical_settlement = {
        "core_epoch": certificate.core_epoch,
        "generation": certificate.generation,
        "actor_token": certificate.actor_token,
        "context_sha256": certificate.context_sha256,
        "provider_token": certificate.provider_token,
        "store_scope_token": certificate.store_scope_token,
        "store_binding_token": certificate.store_binding_token,
        "pins": [pin.document() for pin in pins],
        "reservations": [list(row) for row in reservations],
        "outcomes": [],
        "head_revision": certificate.delivery_revision,
        "head_sha256": certificate.delivery_sha256,
        "last_exact_ack": {
            "revision": certificate.delivery_revision,
            "snapshot_sha256": certificate.delivery_sha256,
            "durable": True,
        },
        "current_write_confirmed": True,
        "recording_unknown": False,
        "capture_failed": False,
        "sealed": False,
    }
    if digest(historical_settlement) != certificate.settlement_sha256:
        raise StorageError("cold historical settlement differs from original startup observation")
    manifest = CarryManifest(
        certificate.core_epoch,
        certificate.generation,
        certificate.actor_token,
        certificate.provider_token,
        certificate.store_scope_token,
        certificate.store_binding_token,
        exclusion_sha256,
        certificate.journal_revision,
        certificate.journal_sha256,
        certificate.settlement_sha256,
        encoded(registry),
        encoded(provenance),
        pins,
        reservations,
        False,
        False,
    )
    if len(encoded(manifest.document()).encode("utf-8")) > min(MAX_CARRY_BYTES, limits.max_bytes):
        raise StorageError("cold carry manifest capacity exhausted")
    return manifest
