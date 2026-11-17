"""Real application owner and pure candidates over a fault-injected atomic CAS port."""

from dataclasses import replace
from decimal import Decimal as D
from threading import Event, Thread

import pytest

from omega.application.margin_account import (
    MarginAccountOwner,
    MarginCommitUnknown,
    MarginOwnerConflict,
    MarginOwnerUnavailable,
)
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.margin import (
    CollateralTransfer,
    FillEvent,
    MarginAccount,
    MarginMode,
    MarginPosition,
    apply_fill,
)
from omega.domain.models import Instrument, Market

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def initial(*, isolated=False, capacity=10000):
    return MarginAccount(
        "a",
        "USDT",
        D(1000),
        (
            MarginPosition(
                BTC, "s", MarginMode.ISOLATED if isolated else MarginMode.CROSS, 10, D(100)
            ),
        ),
        event_capacity=capacity,
    )


def fill(identity="f1", quantity="1", *, when=1):
    return FillEvent(
        identity, "order", BTC, "s", D(quantity), D(100), D(".1"), "USDT", when, when + 1
    )


class Store:
    def __init__(self, value=None):
        self.value, self.loads, self.saves = value, [], []
        self.before_failures = self.ack_losses = 0
        self.read_error, self.hook, self.bad_ack = False, None, False

    def load(self, identity):
        self.loads.append(identity)
        if self.read_error:
            raise StorageError("sensitive raw configuration must not be exposed")
        return self.value

    def save(self, account, *, expected_sequence):
        self.saves.append((account, expected_sequence))
        if self.hook:
            self.hook(account)
        if self.before_failures:
            self.before_failures -= 1
            raise StorageError("before commit")
        if expected_sequence is None:
            assert self.value is None
        elif self.value is None or len(self.value.ledger) != expected_sequence:
            raise ProtocolError("CAS conflict")
        self.value = account
        if self.ack_losses:
            self.ack_losses -= 1
            raise StorageError("commit ACK lost")
        if self.bad_ack:
            return False
        return None


def test_explicit_registration_and_existing_exact_bootstrap_load_do_not_reinitialize_wallet():
    source = initial()
    store = Store()
    owner = MarginAccountOwner(source, store, create_if_missing=True)
    assert owner.snapshot() is source and store.saves == [(source, None)]
    saved = owner.apply_fill(fill()).account
    assert saved.cross_wallet == D("999.9")
    count = len(store.saves)
    restored = MarginAccountOwner(source, store)
    assert restored.snapshot() is saved and len(store.saves) == count
    assert restored.status()["state"] == "ready"


@pytest.mark.parametrize(
    "options", [{}, {"create_if_missing": False}], ids=["default", "explicit-false"]
)
def test_missing_restart_state_is_blocked_without_registration_and_can_load_repaired_file(options):
    source, store = initial(), Store()
    owner = MarginAccountOwner(source, store, **options)
    assert owner.snapshot() is None and owner.pending_snapshot() is None
    assert owner.status()["state"] == "unavailable"
    assert not store.saves
    with pytest.raises(MarginOwnerUnavailable, match="creation"):
        owner.reconcile()
    assert owner.snapshot() is None and not store.saves
    # External explicit file restoration supplies verified actual state. Recovery
    # may read it but cannot manufacture a new opening balance from the template.
    restored = apply_fill(source, fill()).account
    store.value = restored
    assert owner.reconcile() is restored and not store.saves


@pytest.mark.parametrize("permission", [None, 0, 1, "false"])
def test_creation_permission_requires_actual_boolean_before_any_io(permission):
    store = Store()
    with pytest.raises(ValidationError):
        MarginAccountOwner(initial(), store, create_if_missing=permission)
    assert not store.loads and not store.saves


@pytest.mark.parametrize("ack_loss", [False, True], ids=["before-commit", "ack-loss"])
def test_initial_registration_failure_returns_recoverable_owner_without_confirmed_snapshot(
    ack_loss,
):
    store = Store()
    store.ack_losses, store.before_failures = int(ack_loss), int(not ack_loss)
    source = initial()
    owner = MarginAccountOwner(source, store, create_if_missing=True)
    assert owner.snapshot() is None
    assert owner.status()["state"] == "unknown" and not owner.status()["confirmed"]
    pending = owner.pending_snapshot()
    assert (
        pending.previous is None
        and pending.candidate is source
        and pending.expected_sequence is None
    )
    with pytest.raises(ProtocolError, match="blocked"):
        owner.apply_fill(fill())
    assert owner.reconcile() is source
    assert owner.snapshot() is source and owner.pending_snapshot() is None
    assert len(store.saves) == (1 if ack_loss else 2)
    assert all(candidate is source and sequence is None for candidate, sequence in store.saves)


def test_precommit_failure_blocks_new_facts_and_explicit_reconcile_retries_same_candidate_once():
    store = Store()
    owner = MarginAccountOwner(initial(), store, create_if_missing=True)
    before = owner.snapshot()
    store.before_failures = 2
    with pytest.raises(MarginCommitUnknown):
        owner.apply_fill(fill())
    pending = owner.pending_snapshot()
    assert owner.snapshot() is before and store.value is before
    for event in (fill(), fill("new", when=2)):
        with pytest.raises(ProtocolError, match="blocked"):
            owner.apply_fill(event)
    assert len(store.saves) == 2
    with pytest.raises(MarginCommitUnknown):
        owner.reconcile()
    assert owner.pending_snapshot() is pending and len(store.saves) == 3
    assert owner.reconcile() is pending.candidate
    assert store.saves[1:] == [(pending.candidate, 0)] * 3
    assert all(candidate is pending.candidate for candidate, _ in store.saves[1:])


def test_fill_ack_loss_is_confirmed_by_load_without_resaving_or_recomputing_fact_time():
    store = Store()
    owner = MarginAccountOwner(initial(), store, create_if_missing=True)
    previous = owner.snapshot()
    event = fill(when=1234)
    store.ack_losses = 1
    with pytest.raises(MarginCommitUnknown):
        owner.apply_fill(event)
    pending = owner.pending_snapshot()
    assert owner.snapshot() is previous and store.value is pending.candidate
    writes = len(store.saves)
    assert owner.reconcile() is pending.candidate and len(store.saves) == writes
    assert owner.snapshot().ledger[-1].fact is event
    assert owner.snapshot().ledger[-1].fact.available_at_ms == 1235


def test_conflicting_stored_state_blocks_without_rebase_and_keeps_pending_and_last_confirmed():
    store = Store()
    owner = MarginAccountOwner(initial(), store, create_if_missing=True)
    previous = owner.snapshot()
    store.before_failures = 1
    with pytest.raises(MarginCommitUnknown):
        owner.apply_fill(fill())
    pending = owner.pending_snapshot()
    store.value = apply_fill(previous, fill("foreign", "2")).account
    writes = len(store.saves)
    with pytest.raises(MarginOwnerConflict, match="rebase"):
        owner.reconcile()
    assert owner.status()["state"] == "conflict" and owner.snapshot() is previous
    assert owner.pending_snapshot() is pending and len(store.saves) == writes
    with pytest.raises(ProtocolError, match="blocked"):
        owner.apply_fill(fill("later", when=2))


@pytest.mark.parametrize("value", [None, {"corrupt": True}])
def test_missing_or_corrupt_confirmed_load_never_creates_empty_state(value):
    store = Store()
    owner = MarginAccountOwner(initial(), store, create_if_missing=True)
    saved = owner.apply_fill(fill()).account
    writes = len(store.saves)
    store.value = value
    with pytest.raises(MarginOwnerUnavailable):
        owner.reconcile()
    assert owner.snapshot() is saved and owner.status()["state"] == "unavailable"
    assert len(store.saves) == writes


def test_read_failure_is_sanitized_and_pending_is_preserved_until_source_recovers():
    store = Store()
    owner = MarginAccountOwner(initial(), store, create_if_missing=True)
    store.before_failures = 1
    with pytest.raises(MarginCommitUnknown):
        owner.apply_fill(fill())
    pending = owner.pending_snapshot()
    store.read_error = True
    with pytest.raises(MarginOwnerUnavailable):
        owner.reconcile()
    assert "sensitive" not in str(owner.status())
    assert owner.pending_snapshot() is pending
    store.read_error = False
    assert owner.reconcile() is pending.candidate


def test_initial_read_failure_or_v1_shaped_payload_is_blocked_without_implicit_migration():
    for corrupt in (False, True):
        store = Store({"schema_version": 1} if corrupt else None)
        store.read_error = not corrupt
        owner = MarginAccountOwner(initial(), store, create_if_missing=True)
        assert owner.snapshot() is None and owner.status()["state"] == "unavailable"
        assert not store.saves
        store.value, store.read_error = None, False
        assert owner.reconcile() == initial()


@pytest.mark.parametrize(
    "change", ["account", "capacity", "opening", "owner", "mode", "leverage", "budget"]
)
def test_bootstrap_configuration_mismatch_does_not_silently_adopt_saved_account(change):
    source = initial()
    position = source.positions[0]
    if change == "account":
        saved = replace(source, account_id="other")
    elif change == "capacity":
        saved = replace(source, event_capacity=9)
    elif change == "opening":
        saved = replace(source, cross_wallet=D(999), opening_balance=D(999))
    else:
        position = replace(
            position,
            **{
                "owner": {"owner_id": "other"},
                "mode": {"margin_mode": MarginMode.ISOLATED},
                "leverage": {"leverage": 11},
                "budget": {"budget_margin": D(101)},
            }[change],
        )
        saved = replace(source, positions=(position,))
    store = Store(saved)
    owner = MarginAccountOwner(source, store)
    assert owner.status()["state"] == "conflict" and owner.snapshot() is None and not store.saves
    with pytest.raises(MarginOwnerConflict, match="bootstrap"):
        owner.reconcile()


def test_transfer_is_published_only_after_cas_ack_and_identical_fact_never_writes_twice():
    store = Store()
    owner = MarginAccountOwner(initial(isolated=True), store, create_if_missing=True)
    before = owner.snapshot()
    observations = []
    store.hook = lambda candidate: observations.append((owner.snapshot(), candidate))
    event = CollateralTransfer("t1", BTC, "s", D(100), 10, 11)
    result = owner.apply_transfer(event)
    assert observations == [(before, result.account)]
    assert owner.snapshot() is result.account and result.account.cross_wallet == 900
    writes = len(store.saves)
    duplicate = owner.apply_transfer(replace(event, amount=D("100.00")))
    assert duplicate.entry is None and duplicate.account is result.account
    assert len(store.saves) == writes


def test_invalid_domain_fact_or_capacity_exhaustion_does_not_mark_commit_unknown():
    store = Store()
    owner = MarginAccountOwner(initial(capacity=1), store, create_if_missing=True)
    owner.apply_fill(fill())
    writes = len(store.saves)
    with pytest.raises(ProtocolError, match="capacity"):
        owner.apply_fill(fill("new", when=2))
    assert owner.status()["state"] == "ready" and owner.status()["remaining_events"] == 0
    assert len(store.saves) == writes
    assert owner.apply_fill(fill()).entry is None


def test_invalid_save_acknowledgement_never_publishes_until_verified_by_load():
    store = Store()
    owner = MarginAccountOwner(initial(), store, create_if_missing=True)
    before = owner.snapshot()
    store.bad_ack = True
    with pytest.raises(MarginCommitUnknown):
        owner.apply_fill(fill())
    assert owner.snapshot() is before
    assert owner.reconcile() is store.value


def test_interrupted_store_call_retains_pending_and_truthful_unknown_gate():
    store = Store()
    owner = MarginAccountOwner(initial(), store, create_if_missing=True)
    before = owner.snapshot()

    def interrupt(candidate):
        raise KeyboardInterrupt

    store.hook = interrupt
    with pytest.raises(KeyboardInterrupt):
        owner.apply_fill(fill())
    pending = owner.pending_snapshot()
    assert owner.status()["state"] == "unknown" and owner.snapshot() is before
    store.hook = None
    assert owner.reconcile() is pending.candidate


def test_concurrent_facts_are_serialized_through_commit_with_no_uncommitted_publish():
    store = Store()
    owner = MarginAccountOwner(initial(), store, create_if_missing=True)
    before = owner.snapshot()
    entered, release, second_done = Event(), Event(), Event()
    failures = []

    def hook(candidate):
        if len(candidate.ledger) == 1:
            assert owner.snapshot() is before
            entered.set()
            assert release.wait(2)

    def apply(event, done=None):
        try:
            owner.apply_fill(event)
        except Exception as error:
            failures.append(error)
        finally:
            if done:
                done.set()

    store.hook = hook
    first = Thread(target=apply, args=(fill(),))
    second = Thread(target=apply, args=(fill("f2", when=2), second_done))
    try:
        first.start()
        assert entered.wait(2)
        second.start()
        assert not second_done.wait(0.05)
    finally:
        release.set()
        first.join(2)
        if second.ident is not None:
            second.join(2)
    assert not failures and not first.is_alive() and not second.is_alive()
    assert len(owner.snapshot().ledger) == 2 and owner.snapshot().positions[0].quantity == 2
    assert [sequence for _, sequence in store.saves] == [None, 0, 1]
