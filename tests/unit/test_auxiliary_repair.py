"""One-minute repair uses real journals with fake immutable archive/public/revision ports."""

import json
import time
from copy import deepcopy
from dataclasses import replace
from hashlib import sha256

import pytest

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_codec import encode_observation
from omega.infrastructure.binance.auxiliary import agg_trade
from omega.infrastructure.binance.auxiliary_client import AuxiliaryBatch
from omega.ports.auxiliary import AuxiliaryWriteReceipt, StoredAuxiliary
from omega.ports.raw_data import ArchivedRawResponse, RawArchiveReceipt
from omega.runtime.auxiliary_collection import AuxiliaryCollection
from omega.runtime.auxiliary_journal import AuxiliaryJobJournal
from omega.runtime.auxiliary_repair import AuxiliaryRepair
from omega.runtime.auxiliary_service import AuxiliaryService

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
DESTINATION = "a" * 64


def trade(identifier, when):
    return {
        "a": identifier,
        "f": identifier * 2,
        "l": identifier * 2 + 1,
        "p": "100.01",
        "q": ".03",
        "T": when,
        "m": False,
    }


COMPLETE = [trade(0, 59_999), trade(1, 60_001), trade(2, 120_001)]
PARTIAL = [trade(0, 59_999), trade(2, 60_001), trade(3, 120_001)]


class Client:
    def __init__(self, page, now=600_000):
        self.page, self.now, self.calls = page, now, []

    def agg_trades(self, instrument, **options):
        self.calls.append(deepcopy(options))
        if isinstance(self.page, Exception):
            raise self.page
        assert len(self.calls) == 1
        parameters = {
            "symbol": instrument.symbol,
            "limit": str(options["limit"]),
            "startTime": str(options["start_ms"]),
            "endTime": str(options["end_ms"] - 1),
        }
        body = json.dumps(self.page, separators=(",", ":")).encode()
        return AuxiliaryBatch(
            instrument,
            "agg_trades",
            tuple(agg_trade(instrument, row, available_at_ms=self.now) for row in self.page),
            body,
            (),
            self.now,
            tuple(sorted(parameters.items())),
            False,
        )


class Store:
    destination_id = DESTINATION

    def __init__(self):
        self.rows, self.calls = [], []
        self.lose_ack = False

    def put(self, instrument, observation, *, expected_previous=None):
        self.calls.append((observation, expected_previous))
        latest = self.rows[-1] if self.rows else None
        if latest and latest.observation == observation:
            return AuxiliaryWriteReceipt(latest.revision, latest.checksum, False)
        if latest:
            if expected_previous != latest.checksum:
                raise ProtocolError("CAS conflict")
            if observation.available_at_ms <= latest.observation.available_at_ms:
                raise ProtocolError("availability did not advance")
        elif expected_previous is not None:
            raise ProtocolError("missing anchor")
        digest = sha256(encode_observation(instrument, observation)).hexdigest()
        saved = StoredAuxiliary(
            instrument,
            observation,
            1 if latest is None else latest.revision + 1,
            digest,
            None if latest is None else latest.checksum,
        )
        self.rows.append(saved)
        if self.lose_ack:
            self.lose_ack = False
            raise StorageError("lost commit ACK")
        return AuxiliaryWriteReceipt(saved.revision, saved.checksum, True)

    def get(self, instrument, channel, source, opening, *, as_of_ms):
        return next(
            (row for row in reversed(self.rows) if row.observation.available_at_ms <= as_of_ms),
            None,
        )

    def close(self):
        pass


class Archive:
    def __init__(self):
        self.records = {}

    def put(self, record):
        digest = sha256(record.body).hexdigest()
        identity = sha256(
            (digest + repr(record.request_parameters) + str(record.received_at_ms)).encode()
        ).hexdigest()
        receipt = RawArchiveReceipt(
            identity, digest, record.received_at_ms, identity not in self.records
        )
        self.records.setdefault(identity, ArchivedRawResponse(record, receipt))
        return receipt

    def get(self, identity):
        return self.records[identity]


def parent(tmp_path, page=PARTIAL):
    plan = {
        "instrument": INSTRUMENT.key,
        "source": INSTRUMENT.key + ":aggTrade",
        "start_ms": 60_000,
        "end_ms": 120_000,
        "limit": 500,
        "max_pages": 20,
        "destination_id": DESTINATION,
    }
    journal = AuxiliaryJobJournal(tmp_path / "parent", plan)
    archive, store = Archive(), Store()
    result = AuxiliaryCollection(
        plan, Client(page, 300_000), archive, store, journal, clock_ms=lambda: 300_000
    ).run()
    return journal, archive, store, result


def repair(tmp_path, journal, identity="repair_1", **options):
    return AuxiliaryRepair(tmp_path, identity, journal, 60_000, DESTINATION, **options)


def test_observed_repair_appends_revision_and_preserves_parent_raw_pin_and_completed_resume(
    tmp_path,
):
    journal, archive, store, initial = parent(tmp_path)
    parent_bytes = journal.path.read_bytes()
    old, raw_ids = store.rows[0], set(archive.records)
    assert initial["repairable_minutes"] == [
        {"open_time_ms": 60_000, "quality": "partial", "revision": 1, "checksum": old.checksum}
    ]
    attempt, client = repair(tmp_path, journal), Client(COMPLETE)
    result = attempt.run(client, archive, store, clock_ms=lambda: 600_000)
    assert result["complete"] and result["committed"] and result["operation_id"] == "repair_1"
    assert store.rows[0] == old and store.rows[-1].revision == 2
    assert store.rows[-1].previous_checksum == old.checksum
    assert store.rows[-1].observation.quality == "observed"
    assert store.rows[-1].observation.available_at_ms == 600_000
    assert journal.path.read_bytes() == parent_bytes and raw_ids <= set(archive.records)
    calls = len(store.calls)
    resumed = repair(tmp_path, journal).run(
        Client(AssertionError()), archive, store, clock_ms=lambda: 900_000
    )
    assert resumed == result and len(store.calls) == calls


@pytest.mark.parametrize("page", [PARTIAL, []], ids=["partial", "missing"])
def test_another_incomplete_attempt_retains_raw_and_never_stages_or_writes_db(tmp_path, page):
    journal, archive, store, _ = parent(tmp_path)
    attempt = repair(tmp_path, journal)
    before = deepcopy(store.rows)
    calls = len(store.calls)
    result = attempt.run(Client(page), archive, store, clock_ms=lambda: 600_000)
    assert result["reason"] == "repair_incomplete" and not result["committed"]
    assert store.rows == before and len(store.calls) == calls
    saved = AuxiliaryJobJournal(attempt.directory / "journal", attempt.identity["plan"]).snapshot()
    assert saved["batches"] and saved["pending"] is None and saved["deliveries"] == []
    assert (
        repair(tmp_path, journal).run(
            Client(AssertionError()), archive, store, clock_ms=lambda: 900_000
        )
        == result
    )


def test_commit_ack_loss_resumes_frozen_candidate_without_get_or_new_availability(tmp_path):
    journal, archive, store, _ = parent(tmp_path)
    attempt = repair(tmp_path, journal)
    store.lose_ack = True
    first = attempt.run(Client(COMPLETE), archive, store, clock_ms=lambda: 600_000)
    assert first["pending"] and first["commit_outcome"] == "unconfirmed"
    frozen = store.rows[-1]
    client = Client(AssertionError())
    final = repair(tmp_path, journal).run(client, archive, store, clock_ms=lambda: 900_000)
    assert final["complete"] and client.calls == []
    assert len(store.rows) == 2 and store.rows[-1] == frozen
    assert store.calls[-1][0].available_at_ms == 600_000


def test_different_operation_namespace_allows_explicit_new_attempt(tmp_path):
    journal, archive, store, _ = parent(tmp_path)
    first = repair(tmp_path, journal)
    assert (
        first.run(Client(PARTIAL), archive, store, clock_ms=lambda: 600_000)["reason"]
        == "repair_incomplete"
    )
    second = repair(tmp_path, journal, "repair_2")
    assert second.directory != first.directory
    assert second.run(Client(COMPLETE), archive, store, clock_ms=lambda: 900_000)["complete"]


def test_anchor_change_or_wrong_destination_stops_before_public_get(tmp_path):
    journal, archive, store, _ = parent(tmp_path)
    attempt = repair(tmp_path, journal)
    client = Client(COMPLETE)
    store.destination_id = "b" * 64
    with pytest.raises(ValidationError, match="destination"):
        attempt.run(client, archive, store, clock_ms=lambda: 600_000)
    store.destination_id = DESTINATION
    original = store.rows[-1]
    changed = replace(original.observation, available_at_ms=400_000)
    store.put(INSTRUMENT, changed, expected_previous=original.checksum)
    with pytest.raises(ProtocolError, match="parent revision changed"):
        attempt.run(client, archive, store, clock_ms=lambda: 600_000)
    assert client.calls == []


def test_observed_parent_and_conflicting_operation_configuration_are_rejected(tmp_path):
    journal, _, _, _ = parent(tmp_path / "observed", COMPLETE)
    with pytest.raises(ValidationError, match="partial or missing"):
        repair(tmp_path, journal)
    journal, _, _, _ = parent(tmp_path / "partial")
    repair(tmp_path, journal)
    with pytest.raises(StorageError, match="manifest"):
        repair(tmp_path, journal, max_pages=19)


def test_failed_first_get_is_not_automatically_retried_by_resume(tmp_path):
    journal, archive, store, _ = parent(tmp_path)
    attempt = repair(tmp_path, journal)
    initial = Client(StorageError("transport failed"))
    assert not attempt.run(initial, archive, store, clock_ms=lambda: 600_000)["complete"]
    later = Client(COMPLETE)
    result = repair(tmp_path, journal).run(later, archive, store, clock_ms=lambda: 900_000)
    assert not result["complete"] and later.calls == [] and len(store.rows) == 1


def test_legacy_normal_resume_keeps_original_partial_and_does_not_fetch(tmp_path):
    journal, archive, store, initial = parent(tmp_path)
    client = Client(AssertionError())
    final = AuxiliaryCollection(
        journal.snapshot()["plan"], client, archive, store, journal, clock_ms=lambda: 600_000
    ).run()
    assert final == initial and client.calls == [] and len(store.rows) == 1


def test_missing_parent_can_be_repaired_to_proven_observed_zero(tmp_path):
    journal, archive, store, _ = parent(tmp_path, [])
    assert store.rows[0].observation.quality == "missing"
    result = repair(tmp_path, journal).run(
        Client([trade(0, 59_999), trade(1, 120_001)]), archive, store, clock_ms=lambda: 600_000
    )
    assert result["complete"] and store.rows[-1].observation.quality == "observed"
    assert all(value == 0 for name, value in store.rows[-1].observation.values)


def test_delivery_journal_failure_reuses_committed_receipt_without_new_get(tmp_path, monkeypatch):
    journal, archive, store, _ = parent(tmp_path)
    original = AuxiliaryJobJournal._save
    failed = []

    def save(owner, candidate):
        if candidate["deliveries"] and "auxiliary-repairs" in str(owner.path) and not failed:
            failed.append(True)
            raise StorageError("delivery journal failure")
        return original(owner, candidate)

    monkeypatch.setattr(AuxiliaryJobJournal, "_save", save)
    first = repair(tmp_path, journal).run(
        Client(COMPLETE), archive, store, clock_ms=lambda: 600_000
    )
    assert first["commit_outcome"] == "confirmed_pending_delivery"
    count = len(store.calls)
    client = Client(AssertionError())
    final = repair(tmp_path, journal).run(client, archive, store, clock_ms=lambda: 900_000)
    assert final["complete"] and client.calls == [] and len(store.calls) == count
    assert len(store.rows) == 2 and store.rows[-1].observation.available_at_ms == 600_000


def test_incomplete_finish_failure_never_saves_partial_pending(tmp_path, monkeypatch):
    journal, archive, store, _ = parent(tmp_path)
    original = AuxiliaryJobJournal.finish
    failed = []

    def finish(owner, reason, **kwargs):
        if reason == "repair_incomplete" and not failed:
            failed.append(True)
            raise StorageError("finish failure")
        return original(owner, reason, **kwargs)

    monkeypatch.setattr(AuxiliaryJobJournal, "finish", finish)
    attempt = repair(tmp_path, journal)
    count = len(store.calls)
    result = attempt.run(Client(PARTIAL), archive, store, clock_ms=lambda: 600_000)
    saved = AuxiliaryJobJournal(attempt.directory / "journal", attempt.identity["plan"]).snapshot()
    assert not result["pending"] and saved["pending"] is None and len(store.calls) == count


def test_equal_actual_clock_defers_instead_of_inventing_plus_one(tmp_path):
    journal, archive, store, _ = parent(tmp_path)
    attempt = repair(tmp_path, journal)
    first = attempt.run(Client(COMPLETE, 300_000), archive, store, clock_ms=lambda: 300_000)
    assert first["reason"] == "normalization_deferred" and not first["pending"]
    assert len(store.rows) == 1
    client = Client(AssertionError())
    final = repair(tmp_path, journal).run(client, archive, store, clock_ms=lambda: 600_000)
    assert final["complete"] and client.calls == []
    assert store.rows[-1].observation.available_at_ms == 600_000


def test_valid_checksum_cannot_change_immutable_operation_identity(tmp_path):
    journal, archive, store, _ = parent(tmp_path)
    attempt = repair(tmp_path, journal)
    envelope = json.loads(attempt.path.read_bytes())
    envelope["data"]["identity"]["plan"]["destination_id"] = "b" * 64
    encoded = json.dumps(envelope["data"], sort_keys=True, separators=(",", ":")).encode()
    envelope["sha256"] = sha256(encoded).hexdigest()
    attempt.path.write_text(json.dumps(envelope), encoding="utf-8")
    client = Client(COMPLETE)
    with pytest.raises(StorageError, match="manifest"):
        attempt.run(client, archive, store, clock_ms=lambda: 600_000)
    assert client.calls == [] and len(store.rows) == 1


def wait_terminal(service, identity):
    with service.condition:
        deadline = time.monotonic() + 3
        while service.jobs[identity]["state"] in {"queued", "running", "cancelling"}:
            remaining = deadline - time.monotonic()
            assert remaining > 0
            service.condition.wait(min(remaining, 0.05))
    return service.status(identity)


def test_real_service_parent_repair_restart_and_resume_keep_http_and_revision_counts(tmp_path):
    archive, store = Archive(), Store()
    clients, now = {}, [300_000]

    def resources(row):
        client = Client(PARTIAL if row["kind"] == "trade_flow" else COMPLETE, now[0])
        clients.setdefault(row["id"], []).append(client)
        return client, archive, store

    def create():
        return AuxiliaryService(
            tmp_path,
            destination_id=DESTINATION,
            resource_factory=resources,
            clock_ms=lambda: now[0],
        )

    service = create()
    try:
        service.submit(
            "parent",
            "trade_flow",
            {"instrument": INSTRUMENT.key, "start_ms": 60_000, "end_ms": 120_000},
        )
        parent_job = wait_terminal(service, "parent")
        assert parent_job["state"] == "partial"
        parent_json = (
            tmp_path / "state/auxiliary-jobs" / (parent_job["result"]["job_id"] + ".json")
        ).read_bytes()
        now[0] = 600_000
        parameters = {
            "instrument": INSTRUMENT.key,
            "parent_job_id": "parent",
            "parent_execution_id": parent_job["execution_id"],
            "open_time_ms": 60_000,
        }
        service.submit("repair", "trade_flow_repair", parameters)
        repaired = wait_terminal(service, "repair")
        assert repaired["state"] == "completed" and len(store.rows) == 2
        assert (
            tmp_path / "state/auxiliary-jobs" / (parent_job["result"]["job_id"] + ".json")
        ).read_bytes() == parent_json
        call_count = sum(len(client.calls) for rows in clients.values() for client in rows)
        service.close()
        service = create()
        assert service.status("repair")["state"] == "completed"
        assert service.submit("repair", "trade_flow_repair", parameters)["state"] == "completed"
        assert service.resume("repair", repaired["execution_id"])["state"] == "completed"
        assert sum(len(client.calls) for rows in clients.values() for client in rows) == call_count
        assert len(store.rows) == 2
    finally:
        service.close()


def test_service_ack_loss_repair_survives_parent_execution_change_without_get_or_clock_rewrite(
    tmp_path,
):
    archive, store = Archive(), Store()
    clients, now, first_repair = [], [300_000], [True]

    def resources(row):
        client = Client(PARTIAL if row["kind"] == "trade_flow" else COMPLETE, now[0])
        clients.append(client)
        if row["kind"] == "trade_flow_repair" and first_repair[0]:
            store.lose_ack = True
            first_repair[0] = False
        return client, archive, store

    service = AuxiliaryService(
        tmp_path, destination_id=DESTINATION, resource_factory=resources, clock_ms=lambda: now[0]
    )
    try:
        service.submit(
            "parent",
            "trade_flow",
            {"instrument": INSTRUMENT.key, "start_ms": 60_000, "end_ms": 120_000},
        )
        original_parent = wait_terminal(service, "parent")
        now[0] = 600_000
        service.submit(
            "repair",
            "trade_flow_repair",
            {
                "instrument": INSTRUMENT.key,
                "parent_job_id": "parent",
                "parent_execution_id": original_parent["execution_id"],
                "open_time_ms": 60_000,
            },
        )
        first = wait_terminal(service, "repair")
        assert first["state"] == "unknown" and first["result"]["commit_outcome"] == "unconfirmed"
        frozen = store.rows[-1]
        service.resume("parent", original_parent["execution_id"])
        later_parent = wait_terminal(service, "parent")
        assert later_parent["execution_id"] != original_parent["execution_id"]
        count = sum(len(client.calls) for client in clients)
        now[0] = 900_000
        service.resume("repair", first["execution_id"])
        final = wait_terminal(service, "repair")
        assert final["state"] == "completed"
        assert sum(len(client.calls) for client in clients) == count == 2
        assert len(store.rows) == 2 and store.rows[-1] == frozen
        assert store.rows[-1].observation.available_at_ms == 600_000
    finally:
        service.close()
