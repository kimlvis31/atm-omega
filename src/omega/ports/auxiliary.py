"""Revisioned normalized minute observations; provider payloads are a separate concern."""

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument


def _revision_checksum(revision, checksum):
    if (
        type(revision) is not int
        or not 1 <= revision <= 2**63 - 1
        or not isinstance(checksum, str)
        or not re.fullmatch(r"[0-9a-f]{64}", checksum)
    ):
        raise ValidationError("invalid auxiliary revision or SHA256 checksum")


@dataclass(frozen=True, slots=True)
class StoredAuxiliary:
    instrument: Instrument
    observation: AuxiliaryInput
    revision: int
    checksum: str
    previous_checksum: str | None

    def __post_init__(self):
        _revision_checksum(self.revision, self.checksum)
        if (self.revision == 1) != (self.previous_checksum is None):
            raise ValidationError("auxiliary revision predecessor is inconsistent")
        if not isinstance(self.instrument, Instrument) or not isinstance(
            self.observation, AuxiliaryInput
        ):
            raise ValidationError("stored auxiliary requires typed instrument and observation")
        if self.previous_checksum is not None and (
            not isinstance(self.previous_checksum, str)
            or not re.fullmatch(r"[0-9a-f]{64}", self.previous_checksum)
        ):
            raise ValidationError("invalid previous auxiliary checksum")


@dataclass(frozen=True, slots=True)
class AuxiliaryWriteReceipt:
    revision: int
    checksum: str
    inserted: bool

    def __post_init__(self):
        _revision_checksum(self.revision, self.checksum)
        if type(self.inserted) is not bool:
            raise ValidationError("auxiliary inserted receipt must be boolean")


@dataclass(frozen=True, slots=True)
class AuxiliaryReadRange:
    instrument: Instrument
    channel: str
    source: str
    start_ms: int
    end_ms: int

    def __post_init__(self):
        if (
            not isinstance(self.instrument, Instrument)
            or type(self.channel) is not str
            or re.fullmatch(r"[a-z][a-z0-9_]{0,31}", self.channel) is None
            or type(self.source) is not str
            or not 1 <= len(self.source) <= 96
            or not self.source.isprintable()
            or type(self.start_ms) is not int
            or type(self.end_ms) is not int
            or not 0 <= self.start_ms < self.end_ms <= 253_402_300_799_000
            or self.start_ms % 60000
            or self.end_ms % 60000
        ):
            raise ValidationError("invalid bounded auxiliary read range")


class AuxiliaryStore(Protocol):
    @property
    def destination_id(self) -> str:
        """Stable opaque repository identity, excluding credentials."""
        ...

    def put(
        self,
        instrument: Instrument,
        observation: AuxiliaryInput,
        *,
        expected_previous: str | None = None,
    ) -> AuxiliaryWriteReceipt:
        """Append a revision with expected prior checksum; return only after commit.

        Equal decoded observations are idempotent. Revisions retain prior data,
        including its actual availability; they never rewrite historical knowledge.
        Changed observations require strictly later actual availability. A caller
        must record real acceptance/derivation time, never invent an earlier time.
        A byte checksum identifies the canonical normalized encoding, not raw input.
        Conflicts and unknown commits must not publish a successful receipt.
        """
        ...

    def get(
        self,
        instrument: Instrument,
        channel: str,
        source: str,
        open_time_ms: int,
        *,
        as_of_ms: int,
    ) -> StoredAuxiliary | None:
        """Latest eligible revision with available_at<=as_of, or no observation.

        The reader verifies its checksum/chain before exposing normalized data.
        Availability lookup does not imply the observation's quality is usable.
        Pin revision/checksum in replay manifests; this query is not a frozen
        snapshot of the whole dataset and cannot authenticate caller timestamps.
        """
        ...

    def close(self) -> None: ...

    def read_ranges(
        self,
        ranges: tuple[AuxiliaryReadRange, ...],
        *,
        as_of_ms: int,
        limit: int = 10000,
    ) -> tuple[StoredAuxiliary, ...]:
        """Latest known revisions in one read snapshot; no carried/fabricated rows.

        Sum of requested minute slots is at most limit. Exact duplicate or
        overlapping ranges for the same source are rejected, not double counted.
        Each returned revision and its immediate edge is checked. Missing rows
        stay absent and actual availability is retained, including historical
        observations received later than their event time.
        """
        ...

    def read_revisions(
        self,
        ranges: tuple[AuxiliaryReadRange, ...],
        *,
        as_of_ms: int,
        limit: int = 10000,
        check_stop: Callable[[], None] | None = None,
    ) -> tuple[StoredAuxiliary, ...]:
        """All retained revisions known by cutoff in one read-only snapshot.

        Requested minute slots and returned records are each bounded by limit,
        payloads by32MiB. Preserve original revision/edge/availability; never
        truncate silently or substitute a latest-only record for old evidence.
        Optional check_stop raises to abort between queries and fetched rows;
        it cannot interrupt an already executing database statement.
        """
        ...

    def latest_sample(
        self,
        instrument: Instrument,
        channel: str,
        source: str,
        *,
        event_before_ms: int,
        as_of_ms: int,
        max_age_ms: int = 330000,
    ) -> StoredAuxiliary | None:
        """Latest actual point before an exclusive event boundary, known by as_of.

        Point-series windows contain the actual event timestamp. This is not
        a query for synthesized minute carry values. max_age bounds event age
        relative to event_before; consumers also check age at actual use time.
        """
        ...
