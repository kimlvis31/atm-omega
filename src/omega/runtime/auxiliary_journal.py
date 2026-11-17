"""One bounded public collection job: original batches, frozen pending data and pins."""

import json
import re
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

from omega.application.auxiliary_ingestion import AuxiliaryDelivery, PendingAuxiliary
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_codec import decode_observation, encode_observation
from omega.ports.auxiliary import AuxiliaryWriteReceipt
from omega.ports.raw_data import RawArchiveReceipt
from omega.runtime.settings import atomic_json

MAX_BYTES = 2_000_000
_PLAN_KEYS = {"instrument", "source", "start_ms", "end_ms", "limit", "max_pages", "destination_id"}
_STATE_KEYS = {
    "schema_version",
    "plan",
    "job_id",
    "next_ms",
    "batches",
    "pending",
    "deliveries",
    "complete",
    "reason",
}


def _hash(value):
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def _encoded(value):
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def validate_plan(value):
    if type(value) is not dict or set(value) != _PLAN_KEYS:
        raise ValidationError("invalid auxiliary job plan fields")
    try:
        provider, market, symbol = value["instrument"].split(":")
        instrument = Instrument(provider, Market(market), symbol)
    except (ValueError, TypeError, AttributeError):
        raise ValidationError("invalid public auxiliary instrument") from None
    if (
        provider != "binance"
        or instrument.market not in {Market.SPOT, Market.USDM}
        or value["source"] != f"{instrument.key}:aggTrade"
        or not _hash(value["destination_id"])
        or any(type(value[key]) is not int for key in ("start_ms", "end_ms", "limit", "max_pages"))
        or not 0 <= value["start_ms"] < value["end_ms"] <= 253_402_300_799_000
        or value["start_ms"] % 60000
        or value["end_ms"] % 60000
        or value["end_ms"] - value["start_ms"] > 600000
        or not 1 <= value["limit"] <= 1000
        or not 1 <= value["max_pages"] <= 20
    ):
        raise ValidationError(
            "auxiliary plan requires a bounded closed-minute target and destination"
        )
    return deepcopy(value)


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate auxiliary job JSON field")
        result[key] = value
    return result


def _pending(snapshot):
    if snapshot is None:
        return None
    if not isinstance(snapshot, PendingAuxiliary):
        raise ValidationError("typed pending auxiliary snapshot required")
    receipt = snapshot.receipt
    return {
        "payload": encode_observation(snapshot.instrument, snapshot.observation).decode("utf-8"),
        "batch_id": snapshot.batch_id,
        "original_generation": snapshot.original_generation,
        "expected_previous": snapshot.expected_previous,
        "receipt": None
        if receipt is None
        else {
            "revision": receipt.revision,
            "checksum": receipt.checksum,
            "inserted": receipt.inserted,
        },
        "committed_previous": snapshot.committed_previous,
    }


def _restore(value):
    if value is None:
        return None
    if type(value) is not dict or set(value) != {
        "payload",
        "batch_id",
        "original_generation",
        "expected_previous",
        "receipt",
        "committed_previous",
    }:
        raise ValidationError("invalid pending auxiliary journal schema")
    if type(value["payload"]) is not str:
        raise ValidationError("invalid pending auxiliary payload type")
    instrument, observation = decode_observation(value["payload"].encode("utf-8"))
    receipt = value["receipt"]
    if receipt is not None:
        if type(receipt) is not dict or set(receipt) != {"revision", "checksum", "inserted"}:
            raise ValidationError("invalid pending auxiliary receipt schema")
        receipt = AuxiliaryWriteReceipt(**receipt)
    return PendingAuxiliary(
        instrument,
        value["batch_id"],
        observation,
        value["original_generation"],
        value["expected_previous"],
        receipt,
        value["committed_previous"],
    )


class AuxiliaryJobJournal:
    def __init__(self, root: Path, plan: dict):
        plan = validate_plan(plan)
        self.root = Path(root)
        self.job_id = sha256(_encoded(plan)).hexdigest()
        self.path = self.root / (self.job_id + ".json")
        existed = self.path.exists()
        self.data = (
            self._read(self.path)
            if existed
            else {
                "schema_version": 1,
                "plan": plan,
                "job_id": self.job_id,
                "next_ms": plan["start_ms"],
                "batches": [],
                "pending": None,
                "deliveries": [],
                "complete": False,
                "reason": "planned",
            }
        )
        if self.data["plan"] != plan:
            raise ValidationError("auxiliary journal plan does not match its identity")
        if not existed:
            self._save(self.data)

    @classmethod
    def load_plan(cls, root: Path, job_id: str):
        if not _hash(job_id):
            raise ValidationError("auxiliary resume ID must be lowercase SHA256")
        return cls._read(Path(root) / (job_id + ".json"))["plan"]

    @staticmethod
    def _validate(state):
        if type(state) is not dict or set(state) != _STATE_KEYS:
            raise ValidationError("invalid auxiliary journal fields")
        plan = validate_plan(state["plan"])
        if (
            type(state["schema_version"]) is not int
            or state["schema_version"] != 1
            or state["job_id"] != sha256(_encoded(plan)).hexdigest()
            or type(state["next_ms"]) is not int
            or not plan["start_ms"] <= state["next_ms"] <= plan["end_ms"]
            or state["next_ms"] % 60000
            or type(state["batches"]) is not list
            or len(state["batches"]) > plan["max_pages"]
            or any(not _hash(item) for item in state["batches"])
            or len(set(state["batches"])) != len(state["batches"])
            or type(state["deliveries"]) is not list
            or len(state["deliveries"]) > 10
            or type(state["complete"]) is not bool
            or type(state["reason"]) is not str
            or len(state["reason"]) > 160
        ):
            raise ValidationError("invalid auxiliary journal state bounds")
        pending = _restore(state["pending"])
        if (pending is not None or state["deliveries"]) and not state["batches"]:
            raise ValidationError("normalized auxiliary data requires pinned source batches")
        if pending is not None and (
            pending.instrument.key != plan["instrument"]
            or pending.observation.source != plan["source"]
            or pending.observation.window_start_ms != state["next_ms"]
            or pending.observation.window_end_ms > plan["end_ms"]
            or pending.batch_id != f"{state['job_id']}:{state['next_ms']}"
            or state["complete"]
        ):
            raise ValidationError("auxiliary pending snapshot conflicts with its plan")
        cursor = plan["start_ms"]
        for entry in state["deliveries"]:
            snapshot = _restore(entry)
            if (
                snapshot is None
                or snapshot.receipt is None
                or snapshot.instrument.key != plan["instrument"]
                or snapshot.observation.source != plan["source"]
                or snapshot.observation.window_start_ms != cursor
                or snapshot.batch_id != f"{state['job_id']}:{cursor}"
                or (snapshot.receipt.revision == 1) != (snapshot.committed_previous is None)
                or sha256(encode_observation(snapshot.instrument, snapshot.observation)).hexdigest()
                != snapshot.receipt.checksum
            ):
                raise ValidationError("invalid pinned auxiliary delivery")
            cursor += 60000
        if cursor != state["next_ms"] or state["complete"] and cursor != plan["end_ms"]:
            raise ValidationError("auxiliary delivery cursor is inconsistent")

    @classmethod
    def _read(cls, path):
        try:
            if path.is_symlink():
                raise ValueError("journal symlink")
            with path.open("rb") as stream:
                raw = stream.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                raise ValueError("oversized journal")
            envelope = json.loads(raw, object_pairs_hook=_unique)
            if type(envelope) is not dict or set(envelope) != {"data", "sha256"}:
                raise ValueError("invalid journal envelope")
            state = envelope["data"]
            if envelope["sha256"] != sha256(_encoded(state)).hexdigest():
                raise ValueError("journal checksum mismatch")
            cls._validate(state)
            if path.stem != state["job_id"]:
                raise ValueError("journal filename mismatch")
            return state
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            RecursionError,
            UnicodeError,
            ValidationError,
        ):
            raise StorageError("auxiliary job journal is unavailable or invalid") from None

    def _save(self, candidate):
        self._validate(candidate)
        envelope = {"data": candidate, "sha256": sha256(_encoded(candidate)).hexdigest()}
        if len(_encoded(envelope)) > MAX_BYTES:
            raise ValidationError("auxiliary job journal exceeds its size limit")
        try:
            if self.path.is_symlink():
                raise StorageError("auxiliary journal cannot be a symlink")
            atomic_json(self.path, envelope)
        except (OSError, ValueError, TypeError):
            raise StorageError("auxiliary job journal commit failed") from None
        self.data = deepcopy(candidate)

    def snapshot(self):
        return deepcopy(self.data)

    def record_batch(self, receipt):
        if not isinstance(receipt, RawArchiveReceipt) or not _hash(receipt.archive_id):
            raise ValidationError("an archived public batch receipt is required")
        candidate = self.snapshot()
        if receipt.archive_id in candidate["batches"]:
            return
        if candidate["pending"] is not None or candidate["deliveries"]:
            raise ProtocolError("source batch pins are frozen once normalization starts")
        candidate["batches"].append(receipt.archive_id)
        self._save(candidate)

    def stage(self, snapshot):
        if not isinstance(snapshot, PendingAuxiliary):
            raise ValidationError("typed pending auxiliary snapshot required")
        candidate = self.snapshot()
        previous = _restore(candidate["pending"])
        if previous is not None and (
            previous.batch_id != snapshot.batch_id
            or previous.observation != snapshot.observation
            or previous.original_generation != snapshot.original_generation
            or previous.expected_previous != snapshot.expected_previous
            or previous.receipt is not None
            and previous.receipt != snapshot.receipt
            or previous.committed_previous is not None
            and previous.committed_previous != snapshot.committed_previous
        ):
            raise ProtocolError("a different auxiliary minute is already journaled")
        candidate["pending"] = _pending(snapshot)
        candidate["reason"] = "pending_commit" if snapshot.receipt is None else "pending_delivery"
        self._save(candidate)

    def pending(self):
        return _restore(self.data["pending"])

    def delivered(self, delivery):
        if not isinstance(delivery, AuxiliaryDelivery):
            raise ValidationError("typed committed auxiliary delivery required")
        pending = self.pending()
        if (
            pending is None
            or pending.batch_id != delivery.batch_id
            or pending.instrument != delivery.instrument
            or pending.observation != delivery.observation
            or pending.receipt is not None
            and pending.receipt != delivery.receipt
            or pending.committed_previous is not None
            and pending.committed_previous != delivery.previous_checksum
            or type(delivery.generation) is not int
            or delivery.generation < pending.original_generation
        ):
            raise ProtocolError("auxiliary delivery does not match the durable pending record")
        canonical = PendingAuxiliary(
            delivery.instrument,
            delivery.batch_id,
            delivery.observation,
            pending.original_generation,
            pending.expected_previous,
            delivery.receipt,
            delivery.previous_checksum,
        )
        candidate = self.snapshot()
        candidate["deliveries"].append(_pending(canonical))
        candidate["next_ms"] = delivery.observation.window_end_ms
        candidate["pending"], candidate["reason"] = None, "committed_and_pinned"
        self._save(candidate)

    def finish(self, reason: str, *, complete: bool):
        candidate = self.snapshot()
        candidate["reason"], candidate["complete"] = reason, complete
        self._save(candidate)
