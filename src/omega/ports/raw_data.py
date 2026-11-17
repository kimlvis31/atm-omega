"""Original public response bytes are distinct from normalized market observations."""

from dataclasses import dataclass, field
from typing import Protocol

from omega.domain.models import Instrument


@dataclass(frozen=True, slots=True)
class PublicRawResponse:
    instrument: Instrument
    channel: str
    request_parameters: tuple[tuple[str, str], ...]
    received_at_ms: int
    body: bytes = field(repr=False)
    headers: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class RawArchiveReceipt:
    archive_id: str
    body_sha256: str
    received_at_ms: int
    inserted: bool


@dataclass(frozen=True, slots=True)
class ArchivedRawResponse:
    record: PublicRawResponse
    receipt: RawArchiveReceipt


class RawResponseArchive(Protocol):
    def put(self, record: PublicRawResponse) -> RawArchiveReceipt: ...

    def get(self, archive_id: str) -> ArchivedRawResponse: ...
