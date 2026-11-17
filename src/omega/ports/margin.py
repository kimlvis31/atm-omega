"""Atomic state CAS for the opt-in margin ledger; no trading/risk/transport policy."""

from typing import Protocol

from omega.domain.margin import MarginAccount


class MarginStateStore(Protocol):
    def load(self, account_id: str) -> MarginAccount | None:
        """Read a verified complete state, or None only when the account is absent.

        Read/decoding failures must raise. Corruption is never represented as an
        empty account. Implementations preserve exact Decimal values and facts.
        """
        ...

    def save(self, account: MarginAccount, *, expected_sequence: int | None) -> None:
        """Atomically save the whole candidate and return only after durable commit.

        expected_sequence=None means create-if-absent. Otherwise the stored
        ledger length must equal this exact integer. Concurrent changes must
        raise, never silently overwrite. An exception may mean commit ACK loss:
        the caller must load/reconcile before another save or new fact.
        Valid mutations extend the existing fact prefix and preserve bootstrap
        settings. Same-sequence configuration rewrites are not this protocol.
        """
        ...
