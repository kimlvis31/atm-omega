"""Real child journals and fake public/archive/store ports; no network or database."""

import json
import threading
import time
from copy import deepcopy
from hashlib import sha256

import pytest

from omega.domain.errors import ProtocolError, RateLimited, StorageError, ValidationError
from omega.infrastructure.auxiliary_codec import encode_observation
from omega.infrastructure.binance.auxiliary import agg_trade
from omega.infrastructure.binance.auxiliary_client import AuxiliaryBatch
from omega.ports.auxiliary import AuxiliaryWriteReceipt, StoredAuxiliary
from omega.ports.raw_data import ArchivedRawResponse, RawArchiveReceipt
from omega.runtime import auxiliary_schedule as module
from omega.runtime.auxiliary_schedule import AuxiliarySchedule

KEY, DEST = "binance:usdm:ETHUSDT", "a" * 64


def wait_for(predicate):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if predicate():
            return
        threading.Event().wait(0.005)
    assert predicate(), "auxiliary worker did not reach expected bounded state"


def trade(identifier, when):
    return {
        "a": identifier,
        "f": identifier,
        "l": identifier,
        "p": "100",
        "q": "1",
        "T": when,
        "m": False,
    }


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


class Store:
    destination_id = DEST

    def __init__(self):
        self.records, self.puts, self.closes = {}, [], []
        self.lose_ack = False

    def get(self, instrument, channel, source, opening, *, as_of_ms):
        stored = self.records.get((instrument.key, channel, source, opening))
        return stored if stored and stored.observation.available_at_ms <= as_of_ms else None

    def put(self, instrument, observation, *, expected_previous=None):
        self.puts.append(observation)
        key = instrument.key, observation.channel, observation.source, observation.window_start_ms
        previous = self.records.get(key)
        if previous and previous.observation == observation:
            return AuxiliaryWriteReceipt(previous.revision, previous.checksum, False)
        assert (previous.checksum if previous else None) == expected_previous
        digest = sha256(encode_observation(instrument, observation)).hexdigest()
        stored = StoredAuxiliary(
            instrument,
            observation,
            previous.revision + 1 if previous else 1,
            digest,
            expected_previous,
        )
        self.records[key] = stored
        if self.lose_ack:
            self.lose_ack = False
            raise StorageError("synthetic committed write, missing acknowledgement")
        return AuxiliaryWriteReceipt(stored.revision, digest, True)

    def close(self):
        self.closes.append(threading.get_ident())


class Client:
    def __init__(self, now):
        self.now, self.pages, self.calls = now, [], []
        self.metric_value, self.empty_native = "10", False

    def agg_trades(self, instrument, **options):
        self.calls.append(("trade_flow", deepcopy(options)))
        page = self.pages.pop(0)
        if isinstance(page, Exception):
            raise page
        parameters = {"symbol": instrument.symbol, "limit": str(options["limit"])}
        if "from_id" in options:
            parameters["fromId"] = str(options["from_id"])
        else:
            parameters.update(
                startTime=str(options["start_ms"]), endTime=str(options["end_ms"] - 1)
            )
        return AuxiliaryBatch(
            instrument,
            "agg_trades",
            tuple(agg_trade(instrument, row, available_at_ms=self.now[0]) for row in page),
            json.dumps(page).encode(),
            (),
            self.now[0],
            tuple(sorted(parameters.items())),
            False,
        )

    def _native(self, instrument, channel, options):
        self.calls.append((channel, deepcopy(options)))
        stamp = options["start_ms"]
        data = (
            []
            if self.empty_native
            else [
                {
                    "symbol": instrument.symbol,
                    "timestamp": stamp,
                    **(
                        {"sumOpenInterest": self.metric_value, "sumOpenInterestValue": "100"}
                        if channel == "open_interest_history"
                        else {"longShortRatio": "2", "longAccount": ".6", "shortAccount": ".4"}
                    ),
                }
            ]
        )
        parameters = tuple(
            sorted(
                {
                    "symbol": instrument.symbol,
                    "period": "5m",
                    "startTime": str(stamp),
                    "endTime": str(options["end_ms"] - 1),
                    "limit": str(options["limit"]),
                }.items()
            )
        )
        return AuxiliaryBatch(
            instrument, channel, (), json.dumps(data).encode(), (), self.now[0], parameters, False
        )

    def open_interest_history(self, instrument, **options):
        return self._native(instrument, "open_interest_history", options)

    def long_short_ratio(self, instrument, **options):
        return self._native(instrument, "long_short_ratio", options)


def setup(tmp_path, *, now=None, **policy):
    now = now or [180_100]
    client, archive, store, owners = Client(now), Archive(), Store(), []

    def resources(plan):
        owners.append((threading.get_ident(), deepcopy(plan)))
        return client, archive, store

    scheduler = AuxiliarySchedule(
        tmp_path, destination_id=DEST, resource_factory=resources, clock_ms=lambda: now[0], **policy
    )
    return scheduler, now, client, archive, store, owners


def configure(scheduler, channels=("trade_flow",)):
    return scheduler.configure(
        [{"instrument": KEY, "channels": list(channels)}],
        expected_revision=scheduler.snapshot()["revision"],
    )


def row(scheduler, channel="trade_flow"):
    return scheduler.snapshot()["tasks"][KEY + "|" + channel]


def test_constructor_and_configure_do_not_create_resources_and_merge_strategy_demand(tmp_path):
    scheduler, _, _, _, _, owners = setup(tmp_path)
    try:
        assert scheduler.thread is None and owners == []
        result = scheduler.configure(
            [
                {"instrument": KEY, "channels": ["trade_flow"]},
                {"instrument": KEY, "channels": ["metric", "trade_flow"]},
            ],
            expected_revision=0,
        )
        assert result["subscriptions"] == [
            {"instrument": KEY, "channels": ["metric", "trade_flow"]}
        ]
        assert len(result["tasks"]) == 3 and owners == []
        with pytest.raises(ProtocolError, match="revision"):
            scheduler.configure([], expected_revision=0)
    finally:
        scheduler.close()


def test_real_child_commit_and_readback_advance_cursor_and_only_worker_uses_resources(tmp_path):
    scheduler, _, client, _, store, owners = setup(tmp_path)
    client.pages = [[trade(1, 119999), trade(2, 120000)], [trade(3, 180000)]]
    configure(scheduler)
    scheduler.start()
    try:
        wait_for(lambda: row(scheduler)["next_ms"] == 180000)
        assert row(scheduler)["reason"] == "committed"
        assert len(client.calls) == 2 and len(store.records) == 1
        assert all(identity != threading.get_ident() for identity, _ in owners)
        assert store.closes == [owners[0][0]]
        assert len(list((tmp_path / "state/auxiliary-jobs").glob("*.json"))) == 1
    finally:
        scheduler.close()


def test_confirmed_missing_advances_without_automatic_repair_or_zero_fabrication(tmp_path):
    scheduler, _, client, _, store, _ = setup(tmp_path)
    client.pages = [[]]
    configure(scheduler)
    scheduler.start()
    try:
        wait_for(lambda: row(scheduler)["next_ms"] == 180000)
        assert row(scheduler)["reason"] == "partial_committed_manual_repair_only"
        assert next(iter(store.records.values())).observation.quality == "missing"
        assert next(iter(store.records.values())).observation.values == ()
        assert len(client.calls) == 1
    finally:
        scheduler.close()


def test_lost_commit_ack_requires_manual_same_journal_resume_without_more_get(tmp_path):
    scheduler, now, client, _, store, _ = setup(tmp_path, backoff_ms=5)
    client.pages = [[trade(1, 119999), trade(2, 120000)], [trade(3, 180000)]]
    store.lose_ack = True
    configure(scheduler)
    scheduler.start()
    try:
        wait_for(lambda: row(scheduler)["state"] == "unknown" and not scheduler.working)
        assert row(scheduler)["next_ms"] == 120000 and len(client.calls) == 2
        now[0] += 10
        scheduler.resume(KEY, "trade_flow", expected_generation=scheduler.snapshot()["generation"])
        wait_for(lambda: row(scheduler)["next_ms"] == 180000)
        assert len(client.calls) == 2 and len(store.records) == 1
    finally:
        scheduler.close()


def test_public_rate_limit_before_response_automatically_retries_after_finite_backoff(tmp_path):
    scheduler, now, client, _, _, _ = setup(tmp_path, backoff_ms=5, max_retries=1)
    client.pages = [RateLimited(2), []]
    configure(scheduler)
    scheduler.start()
    try:
        wait_for(lambda: row(scheduler)["state"] == "backoff")
        assert len(client.calls) == 1 and row(scheduler)["next_ms"] == 120000
        assert row(scheduler)["retry_at_ms"] == now[0] + 2000
        now[0] = row(scheduler)["retry_at_ms"]
        scheduler.notify()
        wait_for(lambda: row(scheduler)["next_ms"] == 180000)
        assert len(client.calls) == 2
    finally:
        scheduler.close()


def test_native_due_uses_real_sample_and_one_point_windows(tmp_path):
    scheduler, _, client, _, store, _ = setup(tmp_path, now=[315001])
    configure(scheduler, ("metric",))
    scheduler.start()
    try:
        wait_for(lambda: len(store.records) == 2 and not scheduler.working)
        assert len(client.calls) == 2
        assert {obs.observation.event_time_ms for obs in store.records.values()} == {300000}
        assert {obs.observation.available_at_ms for obs in store.records.values()} == {315001}
        assert all(
            options == {"start_ms": 300000, "end_ms": 300001, "limit": 1}
            for _, options in client.calls
        )
    finally:
        scheduler.close()


def test_native_empty_is_retried_within_grace_then_records_missing_point(tmp_path):
    scheduler, now, client, _, store, _ = setup(tmp_path, now=[315001], backoff_ms=5000)
    client.empty_native = True
    configure(scheduler, ("metric",))
    scheduler.start()
    try:
        wait_for(lambda: len(client.calls) == 2 and not scheduler.working)
        assert all(item["state"] == "backoff" for item in scheduler.snapshot()["tasks"].values())
        assert not store.records
        now[0] = 320001
        scheduler.notify()
        wait_for(lambda: len(client.calls) == 4 and not scheduler.working)
        assert all(item["next_ms"] == 300000 for item in scheduler.snapshot()["tasks"].values())
        now[0] = 480001
        scheduler.notify()
        wait_for(
            lambda: all(
                item["next_ms"] == 600000 for item in scheduler.snapshot()["tasks"].values()
            )
        )
        assert not store.records
        assert all(
            item["reason"] == "native_sample_missing_after_grace"
            for item in scheduler.snapshot()["tasks"].values()
        )
    finally:
        scheduler.close()


def test_restart_restores_desired_but_does_not_run_or_reset_cursor(tmp_path):
    scheduler, now, client, archive, store, _ = setup(tmp_path)
    client.pages = [[]]
    configure(scheduler)
    scheduler.start()
    wait_for(lambda: row(scheduler)["next_ms"] == 180000)
    scheduler.close()
    reopened = AuxiliarySchedule(
        tmp_path,
        destination_id=DEST,
        resource_factory=lambda plan: (client, archive, store),
        clock_ms=lambda: now[0],
    )
    try:
        reopened.start()
        assert row(reopened)["next_ms"] == 180000 and not reopened.snapshot()["enabled"]
        assert len(client.calls) == 1
    finally:
        reopened.close()


def test_stop_fences_late_worker_and_closes_resources_on_owner_thread(tmp_path):
    entered, release = threading.Event(), threading.Event()
    scheduler, _, client, archive, store, owners = setup(tmp_path)

    def resources(plan):
        owners.append((threading.get_ident(), plan))
        entered.set()
        assert release.wait(2)
        return client, archive, store

    scheduler.resource_factory = resources
    configure(scheduler)
    scheduler.start()
    try:
        assert entered.wait(2)
        scheduler.stop(expected_generation=scheduler.snapshot()["generation"])
        release.set()
        wait_for(lambda: not scheduler.working)
        assert row(scheduler)["next_ms"] == 120000 and row(scheduler)["state"] == "unknown"
        assert client.calls == [] and store.closes == [owners[0][0]]
    finally:
        release.set()
        scheduler.close()


def test_journal_capacity_is_explicit_block_without_deleting_previous_history(tmp_path):
    scheduler, now, client, _, _, _ = setup(tmp_path, journal_capacity=2)
    client.pages = [[]]
    configure(scheduler)
    scheduler.start()
    try:
        wait_for(lambda: row(scheduler)["next_ms"] == 180000)
        now[0] = 240100
        scheduler.notify()
        wait_for(lambda: row(scheduler)["state"] == "blocked")
        assert row(scheduler)["reason"] == "journal_capacity_reached" and len(client.calls) == 1
        assert len(list((tmp_path / "state/auxiliary-jobs").glob("*.json"))) == 1
    finally:
        scheduler.close()


def test_destination_and_spot_metric_are_rejected_before_public_calls(tmp_path):
    scheduler, _, client, _, store, _ = setup(tmp_path)
    try:
        with pytest.raises(ValidationError, match="unsupported"):
            scheduler.configure(
                [{"instrument": "binance:spot:ETHUSDT", "channels": ["metric"]}],
                expected_revision=0,
            )
        configure(scheduler)
        store.destination_id = "b" * 64
        scheduler.start()
        wait_for(lambda: row(scheduler)["state"] == "unknown")
        assert client.calls == []
    finally:
        scheduler.close()


@pytest.mark.parametrize("changed", [False, True])
def test_repeated_native_point_preserves_availability_or_blocks_changed_values(tmp_path, changed):
    scheduler, now, client, _, store, _ = setup(tmp_path, now=[315001])
    configure(scheduler, ("metric",))
    scheduler.start()
    try:
        wait_for(lambda: len(store.records) == 2 and not scheduler.working)
        original = dict(store.records)
        scheduler.stop()
        scheduler.configure([], expected_revision=scheduler.snapshot()["revision"])
        now[0] += 1000
        client.metric_value = "11" if changed else "10"
        configure(scheduler, ("metric",))
        wait_for(lambda: len(client.calls) == 4 and not scheduler.working)
        assert store.records == original and len(store.puts) == 2
        assert row(scheduler, "open_interest_history")["state"] == (
            "unknown" if changed else "queued"
        )
        assert {value.observation.available_at_ms for value in store.records.values()} == {315001}
    finally:
        scheduler.close()


def test_native_retry_budget_keeps_last_query_at_grace_end_instead_of_early_missing(tmp_path):
    scheduler, now, client, _, store, _ = setup(tmp_path, now=[315001], max_retries=1)
    client.empty_native = True
    configure(scheduler, ("metric",))
    scheduler.start()
    try:
        wait_for(lambda: len(client.calls) == 2 and not scheduler.working)
        assert all(
            item["retry_at_ms"] == 480000 and item["next_ms"] == 300000
            for item in scheduler.snapshot()["tasks"].values()
        )
        now[0] = 479999
        scheduler.notify()
        assert len(client.calls) == 2
        now[0] = 480000
        scheduler.notify()
        wait_for(lambda: len(client.calls) == 4 and not scheduler.working)
        assert all(item["next_ms"] == 600000 for item in scheduler.snapshot()["tasks"].values())
        assert store.records == {}
    finally:
        scheduler.close()


def test_orphan_last_raw_page_is_adopted_from_exact_request_before_resume(tmp_path, monkeypatch):
    scheduler, now, client, _, _, _ = setup(tmp_path, backoff_ms=5)
    client.pages = [[trade(1, 119999), trade(2, 120000)], [trade(3, 180000)]]
    original = module.AuxiliaryJobJournal.record_batch
    failed = []

    def lose_journal(self, receipt):
        if len(self.snapshot()["batches"]) == 1 and not failed:
            failed.append(receipt.archive_id)
            raise StorageError("synthetic journal write failed after raw archive")
        return original(self, receipt)

    monkeypatch.setattr(module.AuxiliaryJobJournal, "record_batch", lose_journal)
    configure(scheduler)
    scheduler.start()
    try:
        wait_for(lambda: row(scheduler)["state"] == "unknown" and not scheduler.working)
        assert len(client.calls) == 2 and failed
        now[0] += 10
        scheduler.resume(KEY, "trade_flow", expected_generation=scheduler.snapshot()["generation"])
        wait_for(lambda: row(scheduler)["next_ms"] == 180000)
        assert len(client.calls) == 2
    finally:
        scheduler.close()


def test_retry_exhaustion_is_blocked_without_an_unbounded_same_get_loop(tmp_path):
    scheduler, now, client, _, _, _ = setup(tmp_path, backoff_ms=5, max_retries=1)
    client.pages = [RateLimited(), RateLimited()]
    configure(scheduler)
    scheduler.start()
    try:
        wait_for(lambda: row(scheduler)["state"] == "backoff")
        now[0] = row(scheduler)["retry_at_ms"]
        scheduler.notify()
        wait_for(lambda: row(scheduler)["state"] == "blocked")
        assert len(client.calls) == 2 and row(scheduler)["next_ms"] == 120000
    finally:
        scheduler.close()


def test_descriptor_corruption_or_destination_change_is_preserved_without_worker(tmp_path):
    scheduler, now, client, archive, store, _ = setup(tmp_path)
    configure(scheduler)
    scheduler.close()
    original = scheduler.path.read_bytes()
    with pytest.raises(StorageError):
        AuxiliarySchedule(
            tmp_path,
            destination_id="b" * 64,
            resource_factory=lambda plan: (client, archive, store),
            clock_ms=lambda: now[0],
        )
    assert scheduler.path.read_bytes() == original
    scheduler.path.write_bytes(b'{"data":{},"sha256":"broken"}')
    damaged = scheduler.path.read_bytes()
    with pytest.raises(StorageError, match="original preserved"):
        AuxiliarySchedule(
            tmp_path, destination_id=DEST, resource_factory=lambda plan: (client, archive, store)
        )
    assert scheduler.path.read_bytes() == damaged


def test_descriptor_ack_loss_is_not_overwritten_by_close(tmp_path, monkeypatch):
    scheduler, _, _, _, _, _ = setup(tmp_path)
    actual = module.atomic_json

    def lose_ack(path, data):
        actual(path, data)
        raise OSError("synthetic descriptor acknowledgement loss")

    monkeypatch.setattr(module, "atomic_json", lose_ack)
    with pytest.raises(StorageError, match="outcome unknown"):
        configure(scheduler)
    saved = scheduler.path.read_bytes()
    with pytest.raises(StorageError, match="unconfirmed"):
        scheduler.close()
    assert scheduler.path.read_bytes() == saved and scheduler.storage_failed


def test_native_publication_recovers_automatically_with_actual_later_availability(tmp_path):
    scheduler, now, client, _, store, _ = setup(tmp_path, now=[315001], backoff_ms=5000)
    client.empty_native = True
    configure(scheduler, ("metric",))
    scheduler.start()
    try:
        wait_for(lambda: len(client.calls) == 2 and not scheduler.working)
        client.empty_native = False
        now[0] = 320001
        scheduler.notify()
        wait_for(lambda: len(store.records) == 2 and not scheduler.working)
        assert all(item["next_ms"] == 600000 for item in scheduler.snapshot()["tasks"].values())
        assert {value.observation.available_at_ms for value in store.records.values()} == {320001}
        assert {value.observation.event_time_ms for value in store.records.values()} == {300000}
    finally:
        scheduler.close()


def test_tampered_fixed_attempt_is_blocked_before_new_factory_or_get(tmp_path):
    scheduler, now, client, _, store, owners = setup(tmp_path, backoff_ms=5)
    client.pages = [[trade(1, 119999), trade(2, 120000)], [trade(3, 180000)]]
    store.lose_ack = True
    configure(scheduler)
    scheduler.start()
    try:
        wait_for(lambda: row(scheduler)["state"] == "unknown" and not scheduler.working)
        path = scheduler._attempt_path(row(scheduler)["attempt"])
        envelope = json.loads(path.read_bytes())
        envelope["data"]["destination_id"] = "b" * 64
        envelope["sha256"] = sha256(module._encoded(envelope["data"])).hexdigest()
        path.write_text(json.dumps(envelope))
        previous_calls, previous_owners = len(client.calls), len(owners)
        now[0] += 10
        scheduler.resume(KEY, "trade_flow", expected_generation=scheduler.snapshot()["generation"])
        wait_for(lambda: not scheduler.snapshot()["storage_available"])
        assert len(client.calls) == previous_calls and len(owners) == previous_owners
    finally:
        with pytest.raises(StorageError, match="unconfirmed"):
            scheduler.close()


def test_stop_during_get_keeps_actual_raw_for_manual_resume_without_late_commit(tmp_path):
    scheduler, now, client, archive, store, _ = setup(tmp_path, backoff_ms=5)
    client.pages = [[trade(1, 119999), trade(2, 120000)], [trade(3, 180000)]]
    entered, release = threading.Event(), threading.Event()
    actual, first = client.agg_trades, [True]

    def delayed(instrument, **options):
        if first[0]:
            first[0] = False
            entered.set()
            assert release.wait(2)
        return actual(instrument, **options)

    client.agg_trades = delayed
    configure(scheduler)
    scheduler.start()
    try:
        assert entered.wait(2)
        scheduler.stop(expected_generation=scheduler.snapshot()["generation"])
        release.set()
        wait_for(lambda: not scheduler.working)
        attempt = scheduler._load_attempt(row(scheduler))
        assert attempt["archive_id"] in archive.records
        assert len(client.calls) == 1 and store.puts == []
        assert row(scheduler)["next_ms"] == 120000
        scheduler.resume(KEY, "trade_flow", expected_generation=scheduler.snapshot()["generation"])
        wait_for(lambda: row(scheduler)["next_ms"] == 180000)
        assert len(client.calls) == 2 and len(store.records) == 1
    finally:
        release.set()
        scheduler.close()


def test_resource_close_failure_releases_owner_and_preserves_committed_child_for_restart(tmp_path):
    scheduler, now, client, archive, store, _ = setup(tmp_path)
    client.pages = [[]]
    actual_close = store.close

    def failed_close():
        actual_close()
        raise StorageError("synthetic resource close failure")

    store.close = failed_close
    configure(scheduler)
    scheduler.start()
    wait_for(lambda: scheduler.storage_failed and not scheduler.thread.is_alive())
    assert row(scheduler)["next_ms"] == 120000 and len(store.records) == 1
    with pytest.raises(StorageError, match="unconfirmed"):
        scheduler.close()
    store.close = actual_close
    reopened = AuxiliarySchedule(
        tmp_path,
        destination_id=DEST,
        resource_factory=lambda plan: (client, archive, store),
        clock_ms=lambda: now[0],
    )
    try:
        reopened.start()
        reopened.resume(KEY, "trade_flow", expected_generation=reopened.snapshot()["generation"])
        wait_for(lambda: row(reopened)["next_ms"] == 180000)
        assert len(client.calls) == 1
    finally:
        reopened.close()


def test_native_unknown_commit_resumes_same_archive_and_clock_without_new_get(tmp_path):
    scheduler, now, client, _, store, _ = setup(tmp_path, now=[315001], backoff_ms=5)
    store.lose_ack = True
    configure(scheduler, ("metric",))
    scheduler.start()
    try:
        wait_for(lambda: len(client.calls) == 2 and not scheduler.working)
        unknown = next(
            value for value in scheduler.snapshot()["tasks"].values() if value["state"] == "unknown"
        )
        assert unknown["next_ms"] == 300000 and len(store.records) == 2
        now[0] += 10
        scheduler.resume(
            KEY, unknown["channel"], expected_generation=scheduler.snapshot()["generation"]
        )
        wait_for(lambda: row(scheduler, unknown["channel"])["next_ms"] == 600000)
        assert len(client.calls) == 2
        assert {value.observation.available_at_ms for value in store.records.values()} == {315001}
    finally:
        scheduler.close()
