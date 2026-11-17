"""Bounded lifecycle with a real temporary journal and fake archive/GET/DB ports."""

import json
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
from threading import Event

import pytest

from omega.domain.errors import RateLimited, StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_codec import encode_observation, observation_checksum
from omega.infrastructure.binance.auxiliary import agg_trade
from omega.infrastructure.binance.auxiliary_client import AuxiliaryBatch
from omega.ports.auxiliary import AuxiliaryWriteReceipt, StoredAuxiliary
from omega.ports.raw_data import ArchivedRawResponse, RawArchiveReceipt
from omega.runtime.auxiliary_collection import AuxiliaryCollection
from omega.runtime.auxiliary_journal import AuxiliaryJobJournal

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
NOW = 300_000
_ARCHIVES = {}


@pytest.fixture(autouse=True)
def isolated_fake_archive_registry():
    _ARCHIVES.clear()
    yield
    _ARCHIVES.clear()


class Archive:
    """A resumed fixture shares prior immutable source records, without OS hardlinks."""

    def __init__(self, root):
        self.records = _ARCHIVES.setdefault(str(root), {})

    def put(self, record):
        digest = sha256(record.body).hexdigest()
        identity = sha256((digest + repr(record.request_parameters)).encode()).hexdigest()
        receipt = RawArchiveReceipt(
            identity, digest, record.received_at_ms, identity not in self.records
        )
        self.records.setdefault(identity, ArchivedRawResponse(record, receipt))
        return receipt

    def get(self, identity):
        return self.records[identity]


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


def plan(**changes):
    return {
        "instrument": INSTRUMENT.key,
        "source": INSTRUMENT.key + ":aggTrade",
        "start_ms": 60_000,
        "end_ms": 180_000,
        "limit": 1000,
        "max_pages": 20,
        "destination_id": "a" * 64,
        **changes,
    }


COMPLETE = [trade(0, 59_999), trade(1, 60_000), trade(2, 120_000), trade(3, 180_000)]


class Client:
    def __init__(self, *pages, stop=None):
        self.pages, self.calls, self.stop = list(pages), [], stop

    def agg_trades(self, instrument, **options):
        self.calls.append(deepcopy(options))
        page = self.pages.pop(0)
        if isinstance(page, Exception):
            raise page
        parameters = {"symbol": instrument.symbol, "limit": options["limit"]}
        if "from_id" in options:
            parameters["fromId"] = options["from_id"]
        else:
            parameters.update(startTime=options["start_ms"], endTime=options["end_ms"] - 1)
        body = json.dumps(page, separators=(",", ":")).encode()
        events = tuple(agg_trade(instrument, row, available_at_ms=NOW) for row in page)
        if self.stop:
            self.stop.set()
        return AuxiliaryBatch(
            instrument,
            "agg_trades",
            events,
            body,
            (),
            NOW,
            tuple(sorted((key, str(value)) for key, value in parameters.items())),
            len(page) == options["limit"],
        )


class Store:
    def __init__(self, *, lost_ack=False, get_failure=False):
        self.values, self.put_calls, self.get_calls = {}, [], []
        self.lost_ack, self.get_failure = lost_ack, get_failure

    def put(self, instrument, observation, *, expected_previous=None):
        self.put_calls.append(observation)
        key = observation.window_start_ms
        existing = self.values.get(key)
        if existing is not None:
            assert existing.observation == observation
            return AuxiliaryWriteReceipt(existing.revision, existing.checksum, False)
        checksum = observation_checksum(encode_observation(instrument, observation))
        self.values[key] = StoredAuxiliary(instrument, observation, 1, checksum, None)
        if self.lost_ack:
            self.lost_ack = False
            raise StorageError("synthetic commit acknowledgement loss")
        return AuxiliaryWriteReceipt(1, checksum, True)

    def get(self, instrument, channel, source, opening, *, as_of_ms):
        self.get_calls.append(opening)
        if self.get_failure:
            self.get_failure = False
            raise StorageError("synthetic postcommit read failure")
        return self.values.get(opening)


def setup(tmp_path, client, *, store=None, job_plan=None, stop=None, clock_ms=lambda: NOW):
    target = job_plan or plan()
    journal = AuxiliaryJobJournal(tmp_path / "jobs", target)
    archive = Archive(tmp_path / "raw")
    store = store or Store()
    runner = AuxiliaryCollection(
        target,
        client,
        archive,
        store,
        journal,
        clock_ms=clock_ms,
        stop_requested=(stop.is_set if stop else lambda: False),
    )
    return runner, journal, archive, store


def test_archive_before_use_full_coverage_pins_every_revision_and_complete_resume_has_no_get(
    tmp_path,
):
    client = Client(COMPLETE)
    runner, journal, archive, store = setup(tmp_path, client)
    result = runner.run()
    assert result["complete"] and result["target_committed"]
    assert result["quality_minutes"] == {"observed": 2, "partial": 0, "missing": 0}
    assert client.calls == [{"limit": 1000, "start_ms": 0, "end_ms": 240_000}]
    state = journal.snapshot()
    assert len(state["batches"]) == 1 and len(state["deliveries"]) == 2
    assert (
        archive.get(state["batches"][0]).record.body
        == json.dumps(COMPLETE, separators=(",", ":")).encode()
    )
    assert all(
        row.observation.mapping()["AGGTRADE_NOTIONALBUY"] == Decimal("3.0003")
        for row in store.values.values()
    )
    repeated, _, _, _ = setup(tmp_path, Client(), store=store)
    assert repeated.run()["complete"] and len(store.put_calls) == 2


def test_additional_pages_use_only_from_id_and_stop_at_actual_after_anchor(tmp_path):
    client = Client(COMPLETE[:2], COMPLETE[2:])
    runner, _, _, _ = setup(tmp_path, client)
    assert runner.run()["complete"]
    assert client.calls[1] == {"limit": 1000, "from_id": 2}


def test_page_cap_commits_explicit_partial_and_never_rewrites_it_on_resume(tmp_path):
    target = plan(max_pages=1)
    runner, journal, _, store = setup(tmp_path, Client(COMPLETE[:3]), job_plan=target)
    result = runner.run()
    assert not result["complete"] and result["target_committed"]
    assert result["quality_minutes"] == {"observed": 1, "partial": 1, "missing": 0}
    assert result["reason"].endswith("page_cap_reached")
    repeated, _, _, _ = setup(tmp_path, Client(), store=store, job_plan=target)
    assert repeated.run() == result and journal.snapshot()["complete"] is False


def test_empty_response_is_archived_but_yields_missing_not_observed_zero(tmp_path):
    runner, _, _, store = setup(tmp_path, Client([]))
    result = runner.run()
    assert result["pages"] == 1 and result["quality_minutes"]["missing"] == 2
    assert not result["complete"] and result["target_committed"]
    assert all(value.observation.values == () for value in store.values.values())


def test_stop_during_get_preserves_body_for_resume_without_second_network_request(tmp_path):
    stop = Event()
    runner, journal, archive, store = setup(tmp_path, Client(COMPLETE, stop=stop), stop=stop)
    stopped = runner.run()
    assert stopped["reason"] == "stopped" and stopped["pages"] == 1
    assert not store.put_calls and archive.get(journal.snapshot()["batches"][0])
    resumed, _, _, _ = setup(tmp_path, Client(), store=store)
    assert resumed.run()["complete"]


def test_stop_after_empty_page_replays_exhaustion_without_repeating_the_same_query(tmp_path):
    stop = Event()
    runner, _, _, store = setup(tmp_path, Client([], stop=stop), stop=stop)
    assert runner.run()["reason"] == "stopped"
    resumed, _, _, _ = setup(tmp_path, Client(), store=store)
    result = resumed.run()
    assert result["target_committed"] and result["quality_minutes"]["missing"] == 2
    assert result["pages"] == 1 and result["reason"].endswith("source_page_empty")


def test_durable_stage_precedes_db_and_stop_preserves_exact_pending_availability(tmp_path):
    stop = Event()
    runner, journal, _, store = setup(tmp_path, Client(COMPLETE), stop=stop)
    original = journal.stage

    def staged(snapshot):
        original(snapshot)
        stop.set()

    journal.stage = staged
    result = runner.run()
    frozen = journal.pending().observation
    assert result["pending"] and not store.put_calls and result["commit_outcome"] == "not_attempted"
    resumed, _, _, _ = setup(tmp_path, Client(), store=store, clock_ms=lambda: NOW + 60_000)
    assert resumed.run()["complete"]
    assert store.put_calls[0].available_at_ms == frozen.available_at_ms == NOW


def test_lost_ack_reuses_frozen_pending_and_does_not_add_duplicate_financial_revision(tmp_path):
    store = Store(lost_ack=True)
    runner, journal, _, _ = setup(tmp_path, Client(COMPLETE), store=store)
    result = runner.run()
    assert result["pending"] and result["commit_outcome"] == "unconfirmed"
    assert journal.pending().observation == store.values[60_000].observation
    resumed, _, _, _ = setup(tmp_path, Client(), store=store, clock_ms=lambda: NOW + 60_000)
    assert resumed.run()["complete"]
    assert len(store.values) == 2 and len(store.put_calls) == 3
    assert store.put_calls[0] == store.put_calls[1]


def test_postcommit_get_failure_retains_receipt_and_resume_does_not_put_again(tmp_path):
    store = Store(get_failure=True)
    runner, journal, _, _ = setup(tmp_path, Client(COMPLETE), store=store)
    result = runner.run()
    assert result["pending"] and result["commit_outcome"] == "confirmed_pending_delivery"
    assert journal.pending().receipt is not None
    resumed, _, _, _ = setup(tmp_path, Client(), store=store)
    assert resumed.run()["complete"] and len(store.put_calls) == 2


def test_raw_archive_failure_prevents_any_event_use_or_db_write(tmp_path):
    runner, journal, archive, store = setup(tmp_path, Client(COMPLETE))
    archive.put = lambda _record: (_ for _ in ()).throw(StorageError("synthetic archive failure"))
    result = runner.run()
    assert result["reason"] == "collection_failed" and not store.put_calls
    assert journal.snapshot()["batches"] == [] and journal.snapshot()["next_ms"] == 60_000


def test_rate_limit_has_no_unbounded_retry_and_leaves_resume_cursor_unchanged(tmp_path):
    client = Client(RateLimited(10))
    runner, journal, _, store = setup(tmp_path, client)
    result = runner.run()
    assert result["reason"] == "rate_limited" and not result["complete"]
    assert (
        len(client.calls) == 1 and not store.put_calls and journal.snapshot()["next_ms"] == 60_000
    )


def test_future_source_time_is_archived_and_deferred_until_real_clock_allows_replay(tmp_path):
    rows = [*COMPLETE[:3], trade(3, NOW + 400)]
    runner, journal, _, store = setup(tmp_path, Client(rows))
    assert runner.run()["reason"] == "normalization_deferred"
    assert len(journal.snapshot()["batches"]) == 1 and not store.put_calls
    resumed, _, _, _ = setup(tmp_path, Client(), store=store, clock_ms=lambda: NOW + 1000)
    assert resumed.run()["complete"]


def test_unclosed_target_is_rejected_before_any_public_get(tmp_path):
    client = Client()
    runner, _, _, _ = setup(tmp_path, client, job_plan=plan(end_ms=360_000))
    with pytest.raises(ValidationError, match="closed"):
        runner.run()
    assert not client.calls
