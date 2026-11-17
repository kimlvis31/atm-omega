"""Bounded content-addressed public wire archive; never rewrites existing bytes.

Files are fsynced before atomic publication. Directory/power-loss durability
depends on the filesystem/OS; this is not a guarantee against hardware failure.
An interrupted two-file publication can leave an unreferenced body, never a
successful receipt for a missing body. Garbage collection is a separate task.
"""

# Imports
import json
import os
import re
import tempfile
from hashlib import sha256
from pathlib import Path

from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.ports.raw_data import ArchivedRawResponse, PublicRawResponse, RawArchiveReceipt

# Constants
MAX_BODY_BYTES = 8_388_608
MAX_METADATA_BYTES = 65_536
_RECEIPT_KEYS = {
    "schema_version",
    "instrument",
    "channel",
    "request_parameters",
    "received_at_ms",
    "body_sha256",
    "headers",
}


def _digest(value):
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def _pairs(value, *, headers=False):
    if type(value) is not tuple or len(value) > 32:
        raise ValidationError("raw archive metadata must be bounded immutable pairs")
    result, seen = [], set()
    for row in value:
        if (
            type(row) is not tuple
            or len(row) != 2
            or not all(type(item) is str for item in row)
            or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", row[0])
            or not 1 <= len(row[1]) <= 2048
            or not row[1].isprintable()
        ):
            raise ValidationError("invalid public archive metadata")
        name = row[0].lower() if headers else row[0]
        if name.lower() in seen:
            raise ValidationError("duplicate public archive metadata name")
        seen.add(name.lower())
        if headers:
            if name not in {"date", "content-type", "retry-after"} and not re.fullmatch(
                r"x-mbx-used-weight(?:-\d+[smhd])?", name
            ):
                raise ValidationError("only approved public response headers may be archived")
        elif any(
            word in name.lower().replace("-", "_")
            for word in ("key", "token", "secret", "password", "signature", "authorization")
        ):
            raise ValidationError("credential fields cannot be archived as public parameters")
        result.append((name, row[1]))
    return tuple(sorted(result))


def _metadata(record):
    if (
        not isinstance(record, PublicRawResponse)
        or not isinstance(record.instrument, Instrument)
        or not isinstance(record.channel, str)
        or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", record.channel)
        or type(record.received_at_ms) is not int
        or not 0 <= record.received_at_ms <= 253_402_300_799_000
        or type(record.body) is not bytes
        or not 1 <= len(record.body) <= MAX_BODY_BYTES
    ):
        raise ValidationError("invalid original public response record")
    metadata = {
        "schema_version": 1,
        "instrument": record.instrument.key,
        "channel": record.channel,
        "request_parameters": _pairs(record.request_parameters),
        "received_at_ms": record.received_at_ms,
        "body_sha256": sha256(record.body).hexdigest(),
        "headers": _pairs(record.headers, headers=True),
    }
    encoded = json.dumps(
        metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    if len(encoded) > MAX_METADATA_BYTES:
        raise ValidationError("raw archive metadata exceeds configured limit")
    return metadata, encoded


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate raw archive metadata")
        result[key] = value
    return result


class FileRawArchive:
    def __init__(self, root: Path):
        self.root = Path(root).absolute()
        self._guard(self.root)
        try:
            self.root.mkdir(parents=True, exist_ok=True)
        except OSError:
            raise StorageError("public response archive root is unavailable") from None

    def _guard(self, path):
        if not path.is_relative_to(self.root):
            raise StorageError("archive path escaped its root")
        current = path
        while current.is_relative_to(self.root):
            if current.is_symlink() or getattr(current, "is_junction", lambda: False)():
                raise StorageError("archive links and junctions are not permitted")
            if current == self.root:
                break
            current = current.parent

    def _path(self, kind, digest):
        if not _digest(digest):
            raise ValidationError("raw archive identifiers must be lowercase SHA256")
        path = self.root / kind / digest[:2] / (digest + (".bin" if kind == "objects" else ".json"))
        self._guard(path)
        return path

    def _read(self, path, limit):
        self._guard(path)
        try:
            with path.open("rb") as stream:
                result = stream.read(limit + 1)
            if len(result) > limit:
                raise StorageError("archived file exceeds its size bound")
            return result
        except OSError:
            raise StorageError("archived public response is unavailable") from None

    def _publish(self, path, data):
        self._guard(path)
        temporary = None
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                if self._read(path, len(data)) != data:
                    raise StorageError("archived content conflict; existing bytes retained")
                return False
            with tempfile.NamedTemporaryFile(
                dir=path.parent, prefix=".raw-", delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, path)
                return True
            except FileExistsError:
                if self._read(path, len(data)) != data:
                    raise StorageError(
                        "concurrent archive conflict; existing bytes retained"
                    ) from None
                return False
        except OSError:
            raise StorageError("public response archive publication failed") from None
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    raise StorageError("public archive temporary cleanup failed") from None

    def put(self, record):
        metadata, encoded = _metadata(record)
        archive_id = sha256(encoded).hexdigest()
        self._publish(self._path("objects", metadata["body_sha256"]), record.body)
        inserted = self._publish(self._path("receipts", archive_id), encoded)
        return RawArchiveReceipt(
            archive_id, metadata["body_sha256"], record.received_at_ms, inserted
        )

    def get(self, archive_id):
        path = self._path("receipts", archive_id)
        encoded = self._read(path, MAX_METADATA_BYTES)
        try:
            if sha256(encoded).hexdigest() != archive_id:
                raise ValueError("receipt checksum mismatch")
            metadata = json.loads(encoded.decode("utf-8"), object_pairs_hook=_unique)
            if type(metadata) is not dict or set(metadata) != _RECEIPT_KEYS:
                raise ValueError("invalid receipt fields")
            if type(metadata["schema_version"]) is not int or metadata["schema_version"] != 1:
                raise ValueError("unknown receipt version")
            provider, market, symbol = metadata["instrument"].split(":")
            body = self._read(self._path("objects", metadata["body_sha256"]), MAX_BODY_BYTES)
            record = PublicRawResponse(
                Instrument(provider, Market(market), symbol),
                metadata["channel"],
                tuple(tuple(row) for row in metadata["request_parameters"]),
                metadata["received_at_ms"],
                body,
                tuple(tuple(row) for row in metadata["headers"]),
            )
            _, expected = _metadata(record)
            if expected != encoded:
                raise ValueError("raw response content checksum or metadata mismatch")
            return ArchivedRawResponse(
                record,
                RawArchiveReceipt(
                    archive_id, metadata["body_sha256"], record.received_at_ms, False
                ),
            )
        except (
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
            UnicodeError,
            RecursionError,
            ValidationError,
        ):
            raise StorageError("archived public response failed integrity checks") from None
