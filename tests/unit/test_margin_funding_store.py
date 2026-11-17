"""Actual account files/CAS over scenario-rate funding; no payment or venue I/O."""

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256

import pytest
from test_margin_store import initial, states
from test_virtual_margin_journal import batch, order, record

from omega.application.margin_account import MarginAccountOwner, MarginCommitUnknown
from omega.application.margin_funding import FundingPoint, prepare_funding_batch
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.margin import apply_funding
from omega.runtime import margin_store as module
from omega.runtime.virtual_margin_journal import encode_record


def funding(account, *, rate=".01", reference="100"):
    points = tuple(
        FundingPoint(position.instrument, 60000, 60010, 60000, D(rate), D(reference))
        for position in sorted(account.positions, key=lambda item: item.instrument.key)
    )
    return prepare_funding_batch(account, points, as_of_ms=120000)


def populate(store):
    for index, account in enumerate(states()):
        store.save(account, expected_sequence=None if index == 0 else index - 1)
    return account


def test_no_funding_account_and_journal_exact_prechange_bytes():
    # Captured from both original codecs before funding codec changes. Domain
    # funding was present but none of these documents contain funding facts.
    account_hashes = (
        "6cf8de30f5148b7d7b9fbcf0962e80a2fe589c682079f412965d7235274d5bbe",
        "43e15d8541904b32e29aa1a6627fec379fdf9c77730d0b540f41e54248deb605",
        "6f7690c883d81210d639516fd9f7dd742d02fb692374ad5b050cb2778cfb8dd1",
        "6b9c25356505cad857064fcb55c61b90fb8959ac14d2429b03c17244cf9a4edd",
    )
    for account, expected in zip(states(), account_hashes, strict=True):
        document = module.account_document(account)
        assert document["schema_version"] == 1
        assert sha256(module._encoded(document)).hexdigest() == expected
    expected = (
        "3228e633bf04dc4351d138981bb3175061aba9651219a7d1cdc0d333cc7fcc9e",
        "6092785c8197794b010f999747e65eb50e395c25b15a2c58acb007f4f1c85b55",
        "3a7a7fa17c17305d10f4987e2813150ee5a6295082f89c5051947daa954aa0e5",
    )
    assert (
        tuple(
            sha256(encode_record(record(value))).hexdigest() for value in (None, order(), batch())
        )
        == expected
    )


@pytest.mark.parametrize(
    "rate,reference", [(".01", "100"), ("-.01", "100"), ("0", "100"), ("1", "10000")]
)
def test_real_mixed_ledger_roundtrip_zero_credit_debit_debt_and_reopen(tmp_path, rate, reference):
    store = module.FileMarginStateStore(tmp_path, "account:1")
    try:
        original = populate(store)
        plan = funding(original, rate=rate, reference=reference)
        for expected in plan.prefix_accounts[1:]:
            store.save(expected, expected_sequence=len(expected.ledger) - 1)
            assert store.load("account:1") == expected
            document = module.account_document(expected)
            assert document["schema_version"] == 2
            assert module.account_from_document(document) == expected
        final = store.load("account:1")
        assert tuple(position.quantity for position in final.positions) == tuple(
            position.quantity for position in original.positions
        )
        assert tuple(position.realized_pnl for position in final.positions) == tuple(
            position.realized_pnl for position in original.positions
        )
        assert len(final.ledger) == len(original.ledger) + 2
        assert all(entry.realized_pnl == entry.fee == 0 for entry in final.ledger[-2:])
        if rate == "1":
            assert final.cross_wallet < 0 and final.positions[1].isolated_wallet < 0
        if rate == "0":
            assert final.cross_wallet == original.cross_wallet
            assert final.positions == original.positions
    finally:
        store.close()
    reopened = module.FileMarginStateStore(tmp_path, "account:1")
    try:
        assert reopened.load("account:1") == final
        owner = MarginAccountOwner(initial(), reopened)
        assert owner.apply_funding(plan.facts[0]).entry is None
        assert owner.apply_funding(plan.facts[1]).entry is None
        assert owner.snapshot() == final
    finally:
        reopened.close()


@pytest.mark.parametrize(
    "mutation",
    [
        "legacy_version",
        "money_number",
        "source_claim",
        "bool_time",
        "missing",
        "extra",
        "kind",
        "amount",
        "policy",
    ],
)
def test_strict_funding_snapshot_rejects_versions_types_formula_and_source(mutation):
    account = funding(states()[-1]).candidate_account
    document = deepcopy(module.account_document(account))
    fact = document["account"]["ledger"][-1]["fact"]
    if mutation == "legacy_version":
        document["schema_version"] = 1
    elif mutation == "money_number":
        fact["amount"] = float(fact["amount"])
    elif mutation == "source_claim":
        fact["source_kind"] = "actual_venue_payment"
    elif mutation == "bool_time":
        fact["available_at_ms"] = True
    elif mutation == "missing":
        del fact["quantum"]
    elif mutation == "extra":
        fact["actual"] = True
    elif mutation == "kind":
        fact["kind"] = "future_cashflow"
    elif mutation == "amount":
        fact["amount"] = "9999"
    else:
        fact["policy_sha256"] = "g" * 64
    with pytest.raises((ValidationError, ProtocolError)):
        module.account_from_document(document)
    original = module.account_document(states()[-1])
    original["schema_version"] = 2
    with pytest.raises(ValidationError):
        module.account_from_document(original)


def test_funding_cas_rejects_wrong_quantity_candidate_without_overwriting_bytes(tmp_path):
    store = module.FileMarginStateStore(tmp_path, "account:1")
    try:
        original = populate(store)
        plan = funding(original)
        candidate = plan.prefix_accounts[1]
        wrong = replace(
            candidate,
            positions=(replace(candidate.positions[0], quantity=D(99)), candidate.positions[1]),
        )
        before = store.path.read_bytes()
        with pytest.raises(ProtocolError, match="appended fact"):
            store.save(wrong, expected_sequence=len(original.ledger))
        assert store.path.read_bytes() == before
        with pytest.raises(ProtocolError):
            store.save(candidate, expected_sequence=len(original.ledger) - 1)
        assert store.path.read_bytes() == before
    finally:
        store.close()


@pytest.mark.parametrize("stage", ["before", "after"])
def test_actual_funding_save_ack_loss_reconciles_exact_candidate_then_restart(
    tmp_path, monkeypatch, stage
):
    store = module.FileMarginStateStore(tmp_path, "account:1")
    original_write = module.atomic_json
    try:
        baseline = populate(store)
        owner = MarginAccountOwner(initial(), store)
        fact = funding(baseline).facts[0]
        expected = apply_funding(baseline, fact).account

        def lost(path, envelope):
            if stage == "after":
                original_write(path, envelope)
            raise OSError("explicit synthetic funding save ACK failure")

        monkeypatch.setattr(module, "atomic_json", lost)
        with pytest.raises(MarginCommitUnknown):
            owner.apply_funding(fact)
        assert owner.snapshot() == baseline
        assert owner.pending_snapshot().candidate == expected
        assert store.load("account:1") == (expected if stage == "after" else baseline)
        monkeypatch.setattr(module, "atomic_json", original_write)
        assert owner.reconcile() == expected
        assert owner.apply_funding(fact).entry is None
    finally:
        store.close()
    restored_store = module.FileMarginStateStore(tmp_path, "account:1")
    try:
        restored = MarginAccountOwner(initial(), restored_store)
        assert restored.snapshot() == expected
        assert restored.apply_funding(fact).entry is None
        assert restored_store.load("account:1") == expected
    finally:
        restored_store.close()
