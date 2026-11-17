"""Single-account factual owner: commit before publish, reconcile ambiguous saves.

This supplies no order approval or source journal. Callers must retain original
fill/transfer facts durably before submitting them, including facts blocked by
capacity or an unknown commit. No callbacks run while the owner lock is held.
"""

from dataclasses import dataclass
from threading import RLock

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.margin import (
    CollateralTransfer,
    FillEvent,
    FundingCashflow,
    MarginAccount,
    MarginTransition,
    apply_fill,
    apply_funding,
    apply_transfer,
)
from omega.ports.margin import MarginStateStore


class MarginCommitUnknown(StorageError):
    """The fixed candidate must be reconciled before accepting another fact."""


class MarginOwnerConflict(ProtocolError):
    """Stored state differs from both the previous and pending states."""


class MarginOwnerUnavailable(StorageError):
    """Verified state could not be read. Never reconstruct it as an empty wallet."""


@dataclass(frozen=True, slots=True)
class PendingMargin:
    previous: MarginAccount | None
    candidate: MarginAccount
    expected_sequence: int | None
    transition: MarginTransition | None


def _identity(account):
    # Wallets, positions and facts evolve. Settings change needs a separate,
    # future explicit configuration operation rather than a restart side effect.
    return (
        account.account_id,
        account.settlement_asset,
        account.event_capacity,
        account.opening_balance,
        tuple(
            sorted(
                (
                    position.instrument.key,
                    position.owner_id,
                    position.margin_mode,
                    position.leverage,
                    position.budget_margin,
                )
                for position in account.positions
            )
        ),
    )


class MarginAccountOwner:
    """Serialize state I/O and mutation with one RLock; publish immutable states.

    Construction returns an unknown owner after initial-registration ACK loss,
    retaining its pending candidate. snapshot() stays None until some state was
    confirmed. Read errors/configuration conflicts also return a blocked owner;
    status() distinguishes them, and explicit reconcile() can retry the read.
    Missing state is unavailable by default. create_if_missing=True is reserved
    for explicitly authorized new accounts, never ordinary restart/recovery.
    """

    def __init__(
        self, initial: MarginAccount, store: MarginStateStore, *, create_if_missing: bool = False
    ):
        if (
            not isinstance(initial, MarginAccount)
            or type(create_if_missing) is not bool
            or not callable(getattr(store, "load", None))
            or not callable(getattr(store, "save", None))
        ):
            raise ValidationError("margin owner requires a typed initial account and state store")
        self._initial, self._store = initial, store
        self._create_if_missing = create_if_missing
        self._lock = RLock()
        self._confirmed, self._pending = None, None
        self._state, self._error = "unavailable", None
        with self._lock:
            try:
                self._bootstrap()
            except (MarginCommitUnknown, MarginOwnerUnavailable, MarginOwnerConflict):
                # Preserve this object/pending for explicit recovery by the caller.
                pass

    def snapshot(self) -> MarginAccount | None:
        with self._lock:
            return self._confirmed

    def pending_snapshot(self) -> PendingMargin | None:
        with self._lock:
            return self._pending

    def status(self):
        with self._lock:
            return {
                "state": self._state,
                "confirmed": self._confirmed is not None,
                "sequence": len(self._confirmed.ledger) if self._confirmed else None,
                "pending": self._pending is not None,
                "pending_sequence": len(self._pending.candidate.ledger) if self._pending else None,
                "remaining_events": (
                    self._confirmed.event_capacity - len(self._confirmed.ledger)
                    if self._confirmed
                    else None
                ),
                "error": self._error,
            }

    def _read(self):
        try:
            loaded = self._store.load(self._initial.account_id)
        except Exception as exc:
            self._state, self._error = "unavailable", type(exc).__name__
            raise MarginOwnerUnavailable("margin state could not be verified") from None
        if loaded is not None and not isinstance(loaded, MarginAccount):
            self._state, self._error = "unavailable", "invalid_state_type"
            raise MarginOwnerUnavailable("margin store returned an invalid typed state")
        if loaded is not None and _identity(loaded) != _identity(self._initial):
            self._state, self._error = "conflict", "bootstrap_identity_changed"
            raise MarginOwnerConflict("margin state has a different bootstrap identity")
        return loaded

    def _publish(self, candidate):
        self._confirmed, self._pending = candidate, None
        self._state, self._error = "ready", None
        return candidate

    def _save_pending(self):
        pending = self._pending
        # Also leave a truthful gate if an interruption is a BaseException rather
        # than a regular adapter exception. The immutable pending survives.
        self._state, self._error = "unknown", None
        try:
            result = self._store.save(
                pending.candidate, expected_sequence=pending.expected_sequence
            )
            if result is not None:
                raise ProtocolError(
                    "margin store did not return its declared commit acknowledgement"
                )
        except Exception as exc:
            self._state, self._error = "unknown", type(exc).__name__
            raise MarginCommitUnknown(
                "margin commit outcome is unknown; reconcile before retry"
            ) from None
        return self._publish(pending.candidate)

    def _bootstrap(self):
        loaded = self._read()
        if loaded is not None:
            return self._publish(loaded)
        if not self._create_if_missing:
            self._state, self._error = "unavailable", "existing_state_missing"
            raise MarginOwnerUnavailable(
                "margin state is absent; explicit creation was not authorized"
            )
        self._pending = PendingMargin(None, self._initial, None, None)
        return self._save_pending()

    def _apply(self, fact, function):
        with self._lock:
            if self._state != "ready" or self._pending is not None or self._confirmed is None:
                raise ProtocolError("margin owner is blocked; explicit reconcile is required")
            transition = function(self._confirmed, fact)
            if transition.entry is None:
                return transition
            self._pending = PendingMargin(
                self._confirmed, transition.account, len(self._confirmed.ledger), transition
            )
            self._save_pending()
            return transition

    def apply_fill(self, fact: FillEvent) -> MarginTransition:
        return self._apply(fact, apply_fill)

    def apply_transfer(self, fact: CollateralTransfer) -> MarginTransition:
        return self._apply(fact, apply_transfer)

    def apply_funding(self, fact: FundingCashflow) -> MarginTransition:
        return self._apply(fact, apply_funding)

    def reconcile(self) -> MarginAccount:
        """Confirm ACK loss, retry exactly once, or block on missing/conflicting state.

        No new fact, candidate recomputation, source timestamp or automatic rebase
        is permitted here. A retry exception leaves the same pending object.
        """
        with self._lock:
            if self._pending is None and self._confirmed is None:
                return self._bootstrap()
            loaded = self._read()
            pending = self._pending
            if pending is not None:
                if loaded == pending.candidate:
                    return self._publish(pending.candidate)
                if loaded == pending.previous:
                    return self._save_pending()
            elif loaded == self._confirmed:
                self._state, self._error = "ready", None
                return self._confirmed
            if loaded is None:
                self._state, self._error = "unavailable", "confirmed_state_missing"
                raise MarginOwnerUnavailable("previously confirmed margin state is missing")
            self._state, self._error = "conflict", "state_changed"
            raise MarginOwnerConflict("margin state changed; automatic rebase is forbidden")
