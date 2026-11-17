"""Actor-owned immutable NN proof/pin records with bounded durable membership.

Release consumes authorization; it never deletes evidence. Limits apply to host
managed records/inputs, not arbitrary filesystem writes by trusted plugin Python.
Uncertain reservations remain charged and are never silently reused or deleted.
"""

from copy import deepcopy
from decimal import Decimal
from pathlib import Path

from omega.domain.errors import StorageError, ValidationError
from omega.domain.external_features import ExternalFeaturePin, ExternalFeatureSnapshot
from omega.domain.inference import (
    CAUSAL_SCOPE,
    InferenceCommitAck,
    InferenceOutputBinding,
    checksum,
    decoded,
    digest,
    encoded,
)
from omega.domain.models import Candle
from omega.research.artifacts import validate_artifact_path
from omega.research.datasets import MarketRow
from omega.runtime.causal_capacity import EvidenceBudgetLimits
from omega.runtime.causal_historical_carry import (
    CarryAcceptedAck,
    CarryInstallRequest,
    verify_carry_install,
)
from omega.runtime.causal_nn import FileInferenceStateStore
from omega.runtime.causal_nn_proof import exclusive_json, read_document

RESERVE_BYTES = 2 * 1024 * 1024


def producer_fingerprint(model_document, contract_sha256, output_bindings_sha256):
    return digest(
        {
            "model": model_document,
            "contract_sha256": contract_sha256,
            "field_bindings_sha256": output_bindings_sha256,
        }
    )


def validate_actual_evidence(snapshot, input_document, receipt, output_bindings):
    """Actual registered bytes bind the portable proof; display dicts cannot."""
    request = input_document.get("request", {})
    if (
        input_document.get("scope") != "committed_inference_input"
        or input_document.get("request_id") != snapshot.request_id
        or digest(request) != snapshot.request_id
        or digest(input_document) != snapshot.evidence_sha256
        or input_document.get("input_sha256") != snapshot.input_sha256
        or digest(input_document.get("snapshot")) != snapshot.input_sha256
        or digest(request.get("frame")) != snapshot.input_frame_sha256
    ):
        raise StorageError("external snapshot differs from actual committed input")
    if (
        receipt.get("scope") != CAUSAL_SCOPE
        or receipt.get("request_id") != snapshot.request_id
        or receipt.get("request_sha256") != snapshot.request_id
        or receipt.get("status") != "complete"
        or digest(receipt) != snapshot.receipt_sha256
    ):
        raise StorageError("external snapshot differs from the actual accepted receipt")
    if (
        (
            request.get("core_epoch"),
            request.get("deployment_epoch"),
            request.get("stream_epoch"),
            request.get("stream"),
        )
        != (
            snapshot.core_epoch,
            snapshot.deployment_epoch,
            snapshot.stream_epoch,
            snapshot.binding.stream,
        )
        or request.get("decision_at_ms") != snapshot.decision_at_ms
        or request.get("deadline_ms") != snapshot.valid_until_ms
        or producer_fingerprint(
            request.get("model"),
            snapshot.binding.contract_sha256,
            request.get("output_bindings_sha256"),
        )
        != snapshot.binding.producer_sha256
    ):
        raise StorageError("external producer/epochs/cutoffs differ from the frozen request")
    if (
        type(output_bindings) is not list
        or not 1 <= len(output_bindings) <= 64
        or digest(output_bindings) != request.get("output_bindings_sha256")
    ):
        raise StorageError("external output mapping differs from frozen request")
    prediction = decoded(receipt["prediction_json"], maximum=262144)
    values, names = [], []
    for row in output_bindings:
        if type(row) is not dict or set(row) != {"name", "path"} or type(row["path"]) is not list:
            raise StorageError("invalid frozen output binding")
        binding = InferenceOutputBinding(row["name"], tuple(row["path"]))
        current = prediction
        for part in binding.path:
            if type(part) is str and type(current) is dict and part in current:
                current = current[part]
            elif type(part) is int and type(current) is list and 0 <= part < len(current):
                current = current[part]
            else:
                raise StorageError("actual receipt is missing its bound output")
        names.append(binding.name)
        values.append(current)
    if tuple(names) != snapshot.binding.fields:
        raise StorageError("external binding fields differ from actual receipt selection")
    numbers = []
    for value in values:
        if value is None:
            numbers.append(None)
            continue
        if type(value) not in {int, float}:
            raise StorageError("external receipt output is not a native finite number")
        number = Decimal(str(value))
        if (
            not number.is_finite()
            or len(number.as_tuple().digits) > 128
            or abs(number.adjusted()) > 100
        ):
            raise StorageError("external receipt output exceeds finite numeric bounds")
        numbers.append(number)
    if snapshot.disposition == "no_signal":
        # The gate commits the recurrent state but suppresses the whole feature
        # when any selected output is absent. Other selected outputs still need
        # the same native finite-number validation as a normal prediction.
        if request.get("model", {}).get("allow_no_signal") is not True or not any(
            value is None for value in numbers
        ):
            raise StorageError("NoSignal snapshot differs from actual mapped receipt")
    elif (
        any(value is None for value in numbers)
        or tuple(zip(names, numbers, strict=True)) != snapshot.values
    ):
        raise StorageError("external feature values differ from actual mapped receipt")
    observations = input_document.get("snapshot", {}).get("observations")
    if type(observations) is not list or not observations:
        raise StorageError("external input has no actual final observed candle")
    last = observations[-1]
    row = MarketRow.parse(last["row"])
    candle = Candle(
        snapshot.binding.instrument,
        row.open_time_ms,
        row.open_time_ms + 60000,
        row.open,
        row.high,
        row.low,
        row.close,
        row.volume,
    )
    if (
        not snapshot.matches_candle(candle)
        or row.instrument != snapshot.binding.instrument.key
        or last.get("origin") != "committed_market"
        or last.get("quality") != "observed"
        or type(last.get("available_at_ms")) is not int
        or last["available_at_ms"] > snapshot.decision_at_ms
    ):
        raise StorageError("external semantic candle does not match its actual frozen input")


class CausalFeatureDeliveryStore:
    def __init__(
        self,
        root,
        *,
        max_bytes=512 * 1024 * 1024,
        max_records=100000,
        max_pins=2048,
        limits=None,
        on_change=None,
        leases=None,
        carry_install=None,
    ):
        if carry_install is not None and type(carry_install) is not CarryInstallRequest:
            raise ValidationError("exact historical carry install request required")
        if limits is not None:
            if type(limits) is not EvidenceBudgetLimits:
                raise ValidationError("parsed immutable evidence limits required")
            if (max_bytes, max_records, max_pins) != (536870912, 100000, 2048):
                raise ValidationError("evidence limits cannot be combined with test overrides")
            max_bytes, max_records, max_pins = (
                limits.max_bytes,
                limits.max_records,
                limits.max_pins,
            )
        if on_change is not None and not callable(on_change):
            raise ValidationError("evidence accounting observer must be callable")
        self.root = Path(root)
        self.directory = self.root / "artifacts/causal-feature-delivery"
        self.max_bytes, self.max_records, self.max_pins = max_bytes, max_records, max_pins
        self.warning_percent = 80 if limits is None else limits.warning_percent
        self._on_change, self._accounting_confirmed = on_change, False
        self._settlement_write_pending = False
        self._settlement_recording_unknown = False
        self._settlement_current_write_confirmed = False
        self._settlement_last_exact_ack = None
        self._carry_install, self._carry_acceptance = carry_install, None
        self.store = FileInferenceStateStore(
            self.root / "state/causal-feature-delivery.json", leases=leases, role="delivery"
        )
        self.revision = None
        try:
            raw = self.store.read()
            if raw is None:
                self.data = {
                    "scope": CAUSAL_SCOPE,
                    "schema_version": 1,
                    "revision": 0,
                    "records": 0,
                    "bytes": 0,
                    "latest": {},
                    "pins": {},
                    "reservations": {},
                }
            else:
                self.data = decoded(raw)
                self._validate()
                self.revision = self.data["revision"]
            if carry_install is not None:
                verify_carry_install(self.root, carry_install, raw, limits=limits)
            self._save()
            if carry_install is not None:
                ack = self._settlement_last_exact_ack
                if (
                    type(self.store) is not FileInferenceStateStore
                    or type(ack) is not InferenceCommitAck
                    or ack is not self.store._last_ack
                    or not self._settlement_current_write_confirmed
                    or self._settlement_recording_unknown
                    or self._settlement_write_pending
                    or ack.revision != carry_install.manifest.delivery_revision + 1
                    or self.store.read() != encoded(self.data)
                ):
                    raise StorageError("original carry writer startup ACK is unconfirmed")
                self._carry_acceptance = CarryAcceptedAck(
                    carry_install.manifest.sha256,
                    carry_install.new_core_epoch,
                    carry_install.new_generation,
                    carry_install.new_actor_token,
                    self.store._lease_scope.token,
                    self.store._lease_binding.token,
                    carry_install.manifest.delivery_revision,
                    carry_install.manifest.delivery_sha256,
                    ack,
                )
        except Exception:
            try:
                self.store.close()
            except Exception:
                # The original store/binding retains independent close evidence;
                # preserve the primary read/commit error for the actor's fault.
                pass
            raise

    def carry_acceptance_snapshot(self):
        """Original constructor-issued immutable cache; no readback or new authority."""
        return self._carry_acceptance

    def _validate(self):
        data = self.data
        if (
            type(data) is not dict
            or set(data)
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
            or data["scope"] != CAUSAL_SCOPE
            or type(data["schema_version"]) is not int
            or data["schema_version"] != 1
        ):
            raise StorageError("invalid external feature membership registry")
        for name in ("revision", "records", "bytes"):
            if type(data[name]) is not int or data[name] < 0:
                raise StorageError("invalid external feature storage counter")
        if (
            type(data["latest"]) is not dict
            or len(data["latest"]) > 128
            or type(data["pins"]) is not dict
            or len(data["pins"]) > self.max_pins
            or type(data["reservations"]) is not dict
            or len(data["reservations"]) > 64
            or data["bytes"] > self.max_bytes + self.max_pins * 2048
            or data["records"] > self.max_records + self.max_pins
        ):
            raise StorageError("external feature registry exceeds bounded membership")
        for key, value in data["latest"].items():
            checksum(key)
            checksum(value)
        for key, value in data["pins"].items():
            checksum(key)
            if type(value) is not dict or set(value) != {"snapshot", "proof_sha256"}:
                raise StorageError("invalid external feature pin membership")
            checksum(value["snapshot"])
            checksum(value["proof_sha256"])
        for value in data["reservations"].values():
            if type(value) is not int or not 0 <= value <= RESERVE_BYTES:
                raise StorageError("invalid external evidence reservation")
        if (
            sum(data["reservations"].values()) > data["bytes"]
            or len(data["reservations"]) > data["records"]
        ):
            raise StorageError("external evidence counters omit uncertain reservations")

    def _save(self):
        candidate = deepcopy(self.data)
        candidate["revision"] += 1
        self._settlement_write_pending = True
        self._settlement_current_write_confirmed = False
        try:
            try:
                ack = self.store.save_snapshot(encoded(candidate), expected_revision=self.revision)
                if (
                    not ack.durable
                    or ack.revision != candidate["revision"]
                    or ack.snapshot_sha256 != digest(candidate)
                ):
                    raise StorageError("external feature membership ACK unconfirmed")
            except Exception:
                self._accounting_confirmed = False
                self._notify_accounting()
                raise
            self.data, self.revision = candidate, candidate["revision"]
            self._accounting_confirmed = True
            # Keep legacy duck-ACK save behavior, while recording only the exact
            # acknowledgement issued by this actual original default file store.
            if (
                type(self.store) is FileInferenceStateStore
                and type(ack) is InferenceCommitAck
                and self.store._last_ack is ack
            ):
                self._settlement_last_exact_ack = ack
                self._settlement_current_write_confirmed = not self._settlement_recording_unknown
            else:
                self._settlement_recording_unknown = True
            self._notify_accounting()
        except BaseException:
            self._settlement_recording_unknown = True
            self._settlement_current_write_confirmed = False
            raise
        else:
            self._settlement_write_pending = False

    def _notify_accounting(self):
        if self._on_change is not None:
            self._on_change(self.capacity_snapshot())

    def capacity_snapshot(self):
        """Actor memory only: cumulative charged host records, never disk/free-space usage."""
        result = {
            "schema_version": 1,
            "scope": "host_causal_evidence_accounting",
            "state": "unavailable",
            "ready": False,
            "accepts_new_reservation": False,
            "accepts_new_pin": False,
            "limits": {
                "max_bytes": self.max_bytes,
                "max_records": self.max_records,
                "max_pins": self.max_pins,
                "warning_percent": self.warning_percent,
            },
            **dict.fromkeys(
                (
                    "charged_bytes",
                    "charged_records",
                    "active_pins",
                    "uncertain_reservations",
                    "uncertain_reserved_bytes",
                    "remaining_bytes",
                    "remaining_records",
                    "remaining_pins",
                )
            ),
            "reservation_headroom_bytes": RESERVE_BYTES,
            "consumed_receipt_allowance_bytes": self.max_pins * 2048,
            "consumed_receipt_allowance_records": self.max_pins,
        }
        if not self._accounting_confirmed:
            return result
        data = self.data
        accepts = (
            data["bytes"] + RESERVE_BYTES <= self.max_bytes
            and data["records"] < self.max_records
            and len(data["reservations"]) < 64
        )
        pin_room = len(data["pins"]) < self.max_pins
        near = any(
            used * 100 >= maximum * self.warning_percent
            for used, maximum in (
                (data["bytes"], self.max_bytes),
                (data["records"], self.max_records),
                (len(data["pins"]), self.max_pins),
            )
        )
        result.update(
            state="full" if not accepts or not pin_room else "warning" if near else "ready",
            ready=True,
            accepts_new_reservation=accepts,
            accepts_new_pin=pin_room and accepts,
            charged_bytes=data["bytes"],
            charged_records=data["records"],
            active_pins=len(data["pins"]),
            uncertain_reservations=len(data["reservations"]),
            uncertain_reserved_bytes=sum(data["reservations"].values()),
            remaining_bytes=max(0, self.max_bytes - data["bytes"]),
            remaining_records=max(0, self.max_records - data["records"]),
            remaining_pins=max(0, self.max_pins - len(data["pins"])),
        )
        return result

    def preflight(self):
        if (
            self.data["bytes"] + RESERVE_BYTES > self.max_bytes
            or self.data["records"] >= self.max_records
            or len(self.data["reservations"]) >= 64
        ):
            raise ValidationError(
                "external feature evidence capacity exhausted; existing proof preserved"
            )

    def _charge(self, key, size, *, release=False):
        if key in self.data["reservations"]:
            if self.data["reservations"][key] != size:
                raise StorageError("external proof reservation conflicts with original bytes")
            return
        extra_bytes = self.max_pins * 2048 if release else 0
        extra_records = self.max_pins if release else 0
        if (
            len(self.data["reservations"]) >= 64
            or size > RESERVE_BYTES
            or self.data["bytes"] + size > self.max_bytes + extra_bytes
            or self.data["records"] >= self.max_records + extra_records
        ):
            raise ValidationError(
                "external proof capacity unavailable; evidence cannot be discarded"
            )
        self.data["reservations"][key] = size
        self.data["bytes"] += size
        self.data["records"] += 1
        self._save()  # Charge before any new immutable bytes can appear.

    def _path(self, kind, key):
        checksum(key)
        return self.directory / kind / (key + ".json")

    def register_input(self, request_id, input_ref):
        """Charge actual bounded input before model dispatch, including failures."""
        checksum(request_id)
        source = validate_artifact_path(self.root / input_ref)
        read_document(source)
        key = "input:" + request_id
        self._charge(key, source.stat().st_size)
        self.data["reservations"].pop(key)
        self._save()

    @staticmethod
    def _put(path, document):
        validate_artifact_path(path, allow_missing=True)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if read_document(path) != document:
                raise StorageError("immutable external evidence changed; original preserved")
        else:
            exclusive_json(path, document)

    def record(self, snapshot, *, input_ref, receipt, output_bindings):
        if not isinstance(snapshot, ExternalFeatureSnapshot):
            raise ValidationError("typed actual external snapshot required")
        ref = Path(input_ref)
        if ref.is_absolute() or ".." in ref.parts:
            raise ValidationError("external input reference must remain project relative")
        source = validate_artifact_path(self.root / ref)
        input_document = read_document(source)
        if (
            digest(input_document) != snapshot.evidence_sha256
            or digest(receipt) != snapshot.receipt_sha256
        ):
            raise StorageError("external snapshot differs from actual input/receipt")
        validate_actual_evidence(snapshot, input_document, receipt, output_bindings)
        body = {
            "scope": "causal_external_snapshot_record",
            "snapshot": snapshot.document(),
            "input_ref": ref.as_posix(),
            "receipt": receipt,
            "output_bindings": output_bindings,
        }
        document = body | {"record_sha256": digest(body)}
        key = snapshot.snapshot_sha256
        path = self._path("snapshots", key)
        if self.data["latest"].get(snapshot.binding.binding_sha256) == key:
            return self.load_snapshot(key)
        reservation = "snapshot:" + key
        self._charge(reservation, len(encoded(document).encode()))
        self._put(path, document)
        # Evict only old cache pointers, never immutable records or pins. Pinned
        # proofs remain independently addressable through their durable pin refs.
        if (
            snapshot.binding.binding_sha256 not in self.data["latest"]
            and len(self.data["latest"]) >= 128
        ):
            self.data["latest"].pop(next(iter(self.data["latest"])))
        self.data["latest"][snapshot.binding.binding_sha256] = key
        self.data["reservations"].pop(reservation)
        self._save()
        return snapshot

    def load_snapshot(self, key):
        document = read_document(self._path("snapshots", key))
        if (
            type(document) is not dict
            or set(document)
            != {"scope", "snapshot", "input_ref", "receipt", "output_bindings", "record_sha256"}
            or document["scope"] != "causal_external_snapshot_record"
            or document["record_sha256"]
            != digest({name: value for name, value in document.items() if name != "record_sha256"})
        ):
            raise StorageError("external immutable snapshot record is invalid")
        try:
            snapshot = ExternalFeatureSnapshot.parse(document["snapshot"])
        except ValidationError:
            raise StorageError("registered external snapshot is malformed") from None
        ref = Path(document["input_ref"])
        if ref.is_absolute() or ".." in ref.parts:
            raise StorageError("external evidence reference escaped its root")
        input_document = read_document(validate_artifact_path(self.root / ref))
        if (
            snapshot.snapshot_sha256 != key
            or digest(input_document) != snapshot.evidence_sha256
            or digest(document["receipt"]) != snapshot.receipt_sha256
        ):
            raise StorageError("actual external source/receipt evidence unavailable")
        validate_actual_evidence(
            snapshot, input_document, document["receipt"], document["output_bindings"]
        )
        return snapshot

    def pin(self, expected):
        path = self._path("pins", expected.pin_id)
        if path.exists():
            pin = self._read_pin(expected.pin_id)
            if pin.pin_id != expected.pin_id:
                raise StorageError("external pin differs from its immutable reference")
        else:
            pin = expected
        released = self._path("consumed", pin.pin_id)
        if released.exists():
            self._read_consumed(pin)
            return pin  # Same historical ACK, never reactivate a consumed pin.
        proof_sha = digest(pin.document())
        previous = self.data["pins"].get(pin.pin_id)
        if previous is not None:
            if previous != {"snapshot": pin.snapshot_sha256, "proof_sha256": proof_sha}:
                raise StorageError("external pin membership conflicts")
            return pin
        if len(self.data["pins"]) >= self.max_pins:
            raise ValidationError("active external pin capacity reached")
        reservation = "pin:" + pin.pin_id
        self._charge(reservation, len(encoded(pin.document()).encode()))
        self._put(path, pin.document())
        self.data["pins"][pin.pin_id] = {"snapshot": pin.snapshot_sha256, "proof_sha256": proof_sha}
        self.data["reservations"].pop(reservation)
        self._save()
        return pin

    def load_pins(self):
        result = {}
        for key, value in self.data["pins"].items():
            pin = self._read_pin(key)
            if (
                pin.pin_id != key
                or digest(pin.document()) != value["proof_sha256"]
                or pin.snapshot_sha256 != value["snapshot"]
            ):
                raise StorageError("actual pin membership proof mismatch")
            if self._path("consumed", key).exists():
                self._read_consumed(pin)
                continue
            result[key] = pin
        return result

    def _read_pin(self, key):
        try:
            return ExternalFeaturePin.parse(read_document(self._path("pins", key)))
        except (ValidationError, ValueError, OSError):
            raise StorageError("actual external pin bytes are unavailable") from None

    def _read_consumed(self, pin):
        document = read_document(self._path("consumed", pin.pin_id))
        if (
            type(document) is not dict
            or set(document) != {"scope", "pin_id", "consumer_receipt_sha256", "record_sha256"}
            or document["scope"] != "consumed_external_feature_pin"
            or document["pin_id"] != pin.pin_id
            or document["record_sha256"]
            != digest({key: value for key, value in document.items() if key != "record_sha256"})
        ):
            raise StorageError("external consumed receipt is invalid")
        checksum(document["consumer_receipt_sha256"])
        return document

    def release(self, pin, consumer_receipt_sha256):
        checksum(consumer_receipt_sha256)
        actual = self._read_pin(pin.pin_id)
        if actual != pin:
            raise StorageError("release differs from the actual external pin")
        path = self._path("consumed", pin.pin_id)
        if path.exists():
            if self._read_consumed(pin)["consumer_receipt_sha256"] != consumer_receipt_sha256:
                raise ValidationError("consumed external pin receipt conflicts")
        else:
            body = {
                "scope": "consumed_external_feature_pin",
                "pin_id": pin.pin_id,
                "consumer_receipt_sha256": consumer_receipt_sha256,
            }
            document = body | {"record_sha256": digest(body)}
            reservation = "release:" + pin.pin_id
            self._charge(reservation, len(encoded(document).encode()), release=True)
            self._put(path, document)
            self.data["reservations"].pop(reservation)
        self.data["pins"].pop(pin.pin_id, None)
        self._save()

    def close(self):
        self.store.close()
