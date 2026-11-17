"""External data authority; pure restored proofs do not register current authority."""

from contextlib import AbstractContextManager
from typing import Protocol

from omega.domain.external_features import (
    ExternalFeatureBinding,
    ExternalFeatureDelivery,
    ExternalFeaturePin,
    ExternalFeatureSnapshot,
)
from omega.domain.models import Candle, Instrument


class ExternalFeatureProvider(Protocol):
    def list_bindings(self) -> tuple[ExternalFeatureBinding, ...]:
        """Bounded cached registered bindings; never inspect a file, DB or model."""
        ...

    def describe(
        self, provider_id: str, instance_id: str, stream: str, instrument: Instrument
    ) -> ExternalFeatureBinding | None:
        """Bounded cached description only: no DB, files or model execution."""
        ...

    def lookup(
        self, binding: ExternalFeatureBinding, candle: Candle, *, decision_at_ms: int
    ) -> ExternalFeatureDelivery:
        """Cached exact-candle proof at the financial cutoff AND actual current clock."""
        ...

    def offer_committed(
        self, binding: ExternalFeatureBinding, candle: Candle
    ) -> ExternalFeatureDelivery:
        """Bounded nonblocking internal event; no caller-controlled operation ID/IO."""
        ...

    def pin(
        self,
        consumer_id: str,
        frame_ref: str,
        snapshot: ExternalFeatureSnapshot,
        candle: Candle,
        *,
        decision_at_ms: int,
    ) -> ExternalFeaturePin:
        """Durable deterministic pin after consumer frame save; verify real authority.

        Same consumer/frame/snapshot retries bind one ID, including an ACK loss.
        Parsing a snapshot or pin document cannot create provider membership.
        """
        ...

    def authorize(
        self,
        pin: ExternalFeaturePin,
        snapshot: ExternalFeatureSnapshot,
        candle: Candle,
        *,
        decision_at_ms: int,
    ) -> bool:
        """Current cached epochs, exact candle/field contract, TTL and actual held pin.

        This is used inside the composition owner's financial entry lease. False
        or exceptions restrict new risk only; financial protection is independent.
        """
        ...

    def entry_lease(
        self,
        pin: ExternalFeaturePin,
        snapshot: ExternalFeatureSnapshot,
        candle: Candle,
        *,
        decision_at_ms: int,
    ) -> AbstractContextManager[bool]:
        """Fence current cached authority through new-risk financial submission.

        Hold only the provider's dedicated cache lock, sample actual TTL inside
        it, and yield authorization. No files, DB, worker wait, pin/release or core
        callback may occur under this lock. Accepted intents/reductions use their
        own financial authority and do not reacquire an external entry lease.
        """
        ...

    def release(self, pin: ExternalFeaturePin, *, consumer_receipt_sha256: str) -> None:
        """Durable final consumer ACK; never garbage-collect solely on feature TTL."""
        ...
