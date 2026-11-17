"""Actual atomic local state with fake public batches/archive/store; no HTTP or DB."""

import json
from decimal import Decimal
from hashlib import sha256

import pytest

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_codec import decode_observation, encode_observation
from omega.infrastructure.binance.auxiliary_client import AuxiliaryBatch
from omega.ports.auxiliary import AuxiliaryWriteReceipt, StoredAuxiliary
from omega.ports.raw_data import ArchivedRawResponse, RawArchiveReceipt
from omega.runtime.auxiliary_statistics import AuxiliaryStatisticsCollector

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
NOW = 900_100


def current(**changes):
    return {"symbol": "ETHUSDT", "openInterest": "123.4500", "time": 900_000, **changes}


def history(timestamp=600_000, **changes):
    return {
        "symbol": "ETHUSDT",
        "sumOpenInterest": "123.45",
        "sumOpenInterestValue": "246.90",
        "timestamp": timestamp,
        **changes,
    }


def ratio(timestamp=600_000):
    return {
        "symbol": "ETHUSDT",
        "longShortRatio": "2",
        "longAccount": ".6667",
        "shortAccount": ".3333",
        "timestamp": timestamp,
    }


class Client:
    def __init__(self, data, *, received=NOW):
        self.data, self.received, self.calls = data, received, []

    def _batch(self, instrument, channel, parameters):
        self.calls.append(channel)
        return AuxiliaryBatch(
            instrument,
            channel,
            (),
            json.dumps(self.data).encode(),
            (),
            self.received,
            tuple(sorted((key, str(value)) for key, value in parameters.items())),
            False,
        )

    def current_open_interest(self, instrument):
        return self._batch(instrument, "open_interest", {"symbol": instrument.symbol})

    def open_interest_history(self, instrument, **parameters):
        return self._batch(
            instrument, "open_interest_history", {"symbol": instrument.symbol, **parameters}
        )

    def long_short_ratio(self, instrument, **parameters):
        return self._batch(
            instrument, "long_short_ratio", {"symbol": instrument.symbol, **parameters}
        )


class Archive:
    def __init__(self):
        self.values = {}

    def put(self, record):
        digest = sha256(record.body).hexdigest()
        identity = sha256(
            (digest + str(record.received_at_ms) + record.channel).encode()
        ).hexdigest()
        receipt = RawArchiveReceipt(
            identity, digest, record.received_at_ms, identity not in self.values
        )
        self.values.setdefault(identity, ArchivedRawResponse(record, receipt))
        return receipt

    def get(self, identity):
        return self.values[identity]


class Store:
    destination_id = "a" * 64

    def __init__(self, *, lost_ack=False, fail_readback=False):
        self.records, self.put_calls = {}, []
        self.lost_ack, self.fail_readback = lost_ack, fail_readback

    def put(self, instrument, observation, *, expected_previous=None):
        self.put_calls.append(observation)
        key = instrument.key, observation.channel, observation.source, observation.window_start_ms
        versions = self.records.setdefault(key, [])
        if versions and versions[-1].observation == observation:
            latest = versions[-1]
            return AuxiliaryWriteReceipt(latest.revision, latest.checksum, False)
        previous = versions[-1] if versions else None
        assert (previous.checksum if previous else None) == expected_previous
        assert (
            previous is None or observation.available_at_ms > previous.observation.available_at_ms
        )
        digest = sha256(encode_observation(instrument, observation)).hexdigest()
        stored = StoredAuxiliary(
            instrument, observation, len(versions) + 1, digest, expected_previous
        )
        versions.append(stored)
        if self.lost_ack:
            self.lost_ack = False
            raise StorageError("synthetic committed write with lost acknowledgement")
        return AuxiliaryWriteReceipt(stored.revision, stored.checksum, True)

    def get(self, instrument, channel, source, opening, *, as_of_ms):
        values = self.records.get((instrument.key, channel, source, opening), [])
        if values and self.fail_readback:
            self.fail_readback = False
            raise StorageError("synthetic read-back failure")
        eligible = [value for value in values if value.observation.available_at_ms <= as_of_ms]
        return eligible[-1] if eligible else None


def service(tmp_path, client, *, archive=None, store=None, clock=lambda: NOW):
    archive, store = archive or Archive(), store or Store()
    return AuxiliaryStatisticsCollector(tmp_path, client, archive, store, clock), archive, store


def fail_clock():
    pytest.fail("a frozen or completed batch must not sample a new clock")


def first_stored(store):
    return next(iter(store.records.values()))[-1]


def test_explicit_recovery_of_archive_before_first_journal_uses_real_new_clock_without_http(
    tmp_path, monkeypatch
):
    collector, archive, store = service(tmp_path, Client(current()))
    original = collector._save

    def fail_initial(state):
        if state["status"] == "archived":
            raise StorageError("synthetic first journal failure after raw receipt")
        original(state)

    monkeypatch.setattr(collector, "_save", fail_initial)
    with pytest.raises(StorageError):
        collector.collect_current(INSTRUMENT)
    identity = next(iter(archive.values))
    assert not collector._path(identity).exists() and store.put_calls == []
    monkeypatch.setattr(collector, "_save", original)
    collector.clock_ms = lambda: NOW + 100
    result = collector.recover_archived(
        identity,
        instrument=INSTRUMENT,
        channel="open_interest",
        request_parameters=(("symbol", INSTRUMENT.symbol),),
    )
    assert result["status"] == "committed" and result["recovered_missing_journal"]
    assert first_stored(store).observation.available_at_ms == NOW + 100
    assert len(collector.client.calls) == 1 and len(store.put_calls) == 1
    collector.clock_ms = fail_clock
    assert (
        collector.recover_archived(
            identity,
            instrument=INSTRUMENT,
            channel="open_interest",
            request_parameters=(("symbol", INSTRUMENT.symbol),),
        )["status"]
        == "committed"
    )
    assert len(store.put_calls) == 1 and len(collector.client.calls) == 1


def test_recovery_never_replaces_a_corrupt_existing_journal(tmp_path):
    collector, _, store = service(tmp_path, Client(current()))
    result = collector.collect_current(INSTRUMENT)
    path = collector._path(result["archive_id"])
    path.write_bytes(b"broken existing journal")
    with pytest.raises(StorageError):
        collector.recover_archived(
            result["archive_id"],
            instrument=INSTRUMENT,
            channel="open_interest",
            request_parameters=(("symbol", INSTRUMENT.symbol),),
        )
    assert path.read_bytes() == b"broken existing journal" and len(store.put_calls) == 1


@pytest.mark.parametrize(
    "channel,parameters",
    [
        ("open_interest_history", (("symbol", INSTRUMENT.symbol),)),
        ("open_interest", (("symbol", "BTCUSDT"),)),
        ("open_interest", (("symbol", INSTRUMENT.symbol), ("limit", "1"))),
    ],
)
def test_missing_journal_recovery_checks_fixed_source_request_before_any_db_write(
    tmp_path, monkeypatch, channel, parameters
):
    collector, archive, store = service(tmp_path, Client(current()))
    monkeypatch.setattr(
        collector, "_save", lambda state: (_ for _ in ()).throw(StorageError("failed"))
    )
    with pytest.raises(StorageError):
        collector.collect_current(INSTRUMENT)
    identity = next(iter(archive.values))
    with pytest.raises(ValidationError):
        collector.recover_archived(
            identity, instrument=INSTRUMENT, channel=channel, request_parameters=parameters
        )
    assert store.put_calls == [] and not collector._path(identity).exists()


def test_current_oi_keeps_actual_point_time_and_does_not_invent_quote_value_or_period(tmp_path):
    client = Client(current())
    collector, archive, store = service(tmp_path, client)
    result = collector.collect_current(INSTRUMENT)
    stored = first_stored(store)
    assert result["status"] == "committed" and result["rows"][0]["revision"] == 1
    assert stored.observation.mapping() == {"METRIC_OPENINTEREST": Decimal("123.4500")}
    assert stored.observation.source == INSTRUMENT.key + ":open_interest"
    assert stored.observation.window_start_ms == stored.observation.event_time_ms == 900_000
    assert stored.observation.available_at_ms == NOW and len(archive.values) == 1
    repeated = AuxiliaryStatisticsCollector(tmp_path, Client(None), archive, store, fail_clock)
    assert repeated.resume(result["archive_id"]) == result and len(store.put_calls) == 1


def test_history_only_stores_native_sparse_samples_and_freezes_all_rows_before_first_put(tmp_path):
    collector, archive, store = service(tmp_path, Client([history(300_123), history(600_123)]))
    original = store.put

    def checked(*args, **options):
        identity = next(iter(archive.values))
        state = collector._read(identity)
        assert state["status"] == "pending" and len(state["rows"]) == 2
        assert all(
            decode_observation(row["payload"].encode())[1].available_at_ms == NOW
            for row in state["rows"]
        )
        return original(*args, **options)

    store.put = checked
    result = collector.collect_history(INSTRUMENT, 0, 900_000, 10)
    assert result["status"] == "committed" and len(store.records) == 2
    for rows in store.records.values():
        observation = rows[-1].observation
        assert observation.event_time_ms % 300000 == 123
        assert observation.window_start_ms == observation.event_time_ms - 123
        assert observation.mapping()["SOURCE_PERIOD_MS"] == 300000
        assert observation.mapping()["METRIC_OPENINTERESTVALUE"] == Decimal("246.90")
        assert observation.source.endswith(":open_interest_history")


def test_ratio_fractions_and_real_five_minute_metadata_are_distinct_from_oi(tmp_path):
    collector, _, store = service(tmp_path, Client([ratio()]))
    collector.collect_ratios(INSTRUMENT, 0, 900000, 10)
    observation = first_stored(store).observation
    assert observation.channel == "long_short_ratio"
    assert observation.mapping() == {
        "METRIC_LONGSHORTRATIO": Decimal(2),
        "METRIC_LONGACCOUNT": Decimal(".6667"),
        "METRIC_SHORTACCOUNT": Decimal(".3333"),
        "SOURCE_PERIOD_MS": 300000,
    }


def test_repeated_same_point_reuses_actual_stored_availability_without_new_revision(tmp_path):
    collector, archive, store = service(tmp_path, Client(current()))
    collector.collect_current(INSTRUMENT)
    first = first_stored(store)
    collector = AuxiliaryStatisticsCollector(
        tmp_path, Client(current(), received=NOW + 100), archive, store, lambda: NOW + 100
    )
    result = collector.collect_current(INSTRUMENT)
    assert result["status"] == "committed" and len(archive.values) == 2
    assert len(store.put_calls) == 1 and first_stored(store) == first
    assert collector._read(result["archive_id"])["rows"][0]["receipt"]["inserted"] is False


def test_changed_value_same_minute_uses_cas_and_strictly_later_actual_availability(tmp_path):
    collector, archive, store = service(tmp_path, Client(current()))
    collector.collect_current(INSTRUMENT)
    original = first_stored(store)
    collector = AuxiliaryStatisticsCollector(
        tmp_path,
        Client(current(openInterest="124"), received=NOW + 100),
        archive,
        store,
        lambda: NOW + 100,
    )
    result = collector.collect_current(INSTRUMENT)
    updated = first_stored(store)
    assert updated.revision == 2 and updated.previous_checksum == original.checksum
    assert updated.observation.available_at_ms == NOW + 100 and result["status"] == "committed"


def test_new_provider_point_inside_same_minute_is_a_real_revision_even_with_unchanged_amount(
    tmp_path,
):
    collector, archive, store = service(tmp_path, Client(current()))
    collector.collect_current(INSTRUMENT)
    collector = AuxiliaryStatisticsCollector(
        tmp_path,
        Client(current(time=900_050), received=NOW + 100),
        archive,
        store,
        lambda: NOW + 100,
    )
    collector.collect_current(INSTRUMENT)
    updated = first_stored(store)
    assert updated.revision == 2 and updated.observation.event_time_ms == 900_050
    assert updated.observation.window_start_ms == 900_000


def test_same_clock_changed_value_defers_without_faking_plus_one(tmp_path):
    collector, archive, store = service(tmp_path, Client(current()))
    collector.collect_current(INSTRUMENT)
    collector = AuxiliaryStatisticsCollector(
        tmp_path, Client(current(openInterest="124")), archive, store, lambda: NOW
    )
    result = collector.collect_current(INSTRUMENT)
    assert result["status"] == "archived" and len(store.put_calls) == 1
    assert result["reason"] == "normalization_requires_strictly_later_clock"
    collector.clock_ms = lambda: (
        NOW + 1
    )  # A later actual clock sample, not a fabricated stored value.
    assert collector.resume(result["archive_id"])["status"] == "committed"
    assert first_stored(store).observation.available_at_ms == NOW + 1


def test_future_sample_is_archived_then_normalized_only_when_actual_clock_reaches_it(tmp_path):
    collector, archive, store = service(tmp_path, Client(current(time=NOW + 400)))
    result = collector.collect_current(INSTRUMENT)
    assert result["status"] == "archived" and result["rows"] == [] and not store.put_calls
    assert result["deferred_until_ms"] == NOW + 400
    collector.clock_ms = lambda: NOW + 1000
    assert collector.resume(result["archive_id"])["status"] == "committed"
    observation = first_stored(store).observation
    assert observation.event_time_ms == NOW + 400 and observation.available_at_ms == NOW + 1000
    assert len(archive.values) == 1


def test_lost_commit_ack_resume_uses_exact_frozen_bytes_without_clock_or_http(tmp_path):
    store = Store(lost_ack=True)
    client = Client(current())
    collector, archive, _ = service(tmp_path, client, store=store)
    with pytest.raises(StorageError):
        collector.collect_current(INSTRUMENT)
    identity = next(iter(archive.values))
    state = collector._read(identity)
    assert state["status"] == "pending" and state["rows"][0]["receipt"] is None
    original_payload = state["rows"][0]["payload"]
    collector.clock_ms = fail_clock
    assert collector.resume(identity)["status"] == "committed"
    assert len(client.calls) == 1 and store.put_calls[0] == store.put_calls[1]
    assert collector._read(identity)["rows"][0]["payload"] == original_payload


def test_readback_failure_preserves_receipt_and_resume_queries_without_second_put(tmp_path):
    store = Store(fail_readback=True)
    collector, archive, _ = service(tmp_path, Client(current()), store=store)
    with pytest.raises(StorageError):
        collector.collect_current(INSTRUMENT)
    identity = next(iter(archive.values))
    assert collector._read(identity)["rows"][0]["receipt"] is not None
    collector.clock_ms = fail_clock
    assert collector.resume(identity)["status"] == "committed" and len(store.put_calls) == 1


def test_empty_native_history_is_archived_and_committed_without_zero_samples(tmp_path):
    collector, archive, store = service(tmp_path, Client([]))
    result = collector.collect_history(INSTRUMENT, 0, 900000, 10)
    assert result["status"] == "committed" and result["rows"] == []
    assert len(archive.values) == 1 and not store.put_calls


def test_clock_regression_leaves_archived_source_without_partial_frozen_rows(tmp_path):
    clock = iter((NOW, NOW - 1))
    collector, _, store = service(tmp_path, Client(current()), clock=lambda: next(clock))
    result = collector.collect_current(INSTRUMENT)
    assert result["status"] == "archived" and not store.put_calls
    assert result["deferred_until_ms"] == NOW


def test_bad_sample_duplicate_minute_and_spot_capability_fail_before_any_db_write(tmp_path):
    for data in ([history(), history()], [{"symbol": "BTCUSDT"}], [history(sumOpenInterest="NaN")]):
        collector, _, store = service(tmp_path, Client(data))
        with pytest.raises((ProtocolError, ValidationError)):
            collector.collect_history(INSTRUMENT, 0, 900000, 10)
        assert not store.put_calls
    collector, _, _ = service(tmp_path, Client(None))
    with pytest.raises(ValidationError):
        collector.collect_current(Instrument("binance", Market.SPOT, "ETHUSDT"))


def test_archive_or_journal_failure_prevents_normalized_store_mutation(tmp_path):
    collector, archive, store = service(tmp_path, Client(current()))
    archive.put = lambda _: (_ for _ in ()).throw(StorageError("synthetic archive error"))
    with pytest.raises(StorageError):
        collector.collect_current(INSTRUMENT)
    assert not store.put_calls
    collector, _, store = service(tmp_path / "second", Client(current()))
    collector._save = lambda _: (_ for _ in ()).throw(StorageError("synthetic journal error"))
    with pytest.raises(StorageError):
        collector.collect_current(INSTRUMENT)
    assert not store.put_calls


def test_completed_pin_corruption_or_path_escape_is_rejected(tmp_path):
    collector, _, _ = service(tmp_path, Client(current()))
    result = collector.collect_current(INSTRUMENT)
    path = collector._path(result["archive_id"])
    envelope = json.loads(path.read_bytes())
    envelope["data"]["rows"][0]["checksum"] = "f" * 64
    path.write_text(json.dumps(envelope), encoding="utf-8")
    with pytest.raises(StorageError):
        collector.resume(result["archive_id"])
    with pytest.raises(ValidationError):
        collector.resume("../escape")


def test_dataset_owner_lock_prevents_overlapping_current_sample_fetch(tmp_path):
    collector, _, _ = service(tmp_path, Client(current()))
    owner = collector._lock(INSTRUMENT, "open_interest")
    try:
        with pytest.raises(RuntimeError):
            collector.collect_current(INSTRUMENT)
    finally:
        owner.close()


def test_constructor_rejects_missing_or_mismatched_destination_identity_before_io(tmp_path):
    with pytest.raises(ValidationError, match="destination"):
        AuxiliaryStatisticsCollector(tmp_path, Client(None), Archive(), object(), fail_clock)
    with pytest.raises(ValidationError, match="destination"):
        AuxiliaryStatisticsCollector(
            tmp_path, Client(None), Archive(), Store(), fail_clock, destination_id="b" * 64
        )
    assert not (tmp_path / "state").exists()


def test_pending_resume_refuses_another_destination_before_clock_archive_or_db(tmp_path):
    store = Store(lost_ack=True)
    collector, archive, _ = service(tmp_path, Client(current()), store=store)
    with pytest.raises(StorageError):
        collector.collect_current(INSTRUMENT)
    other_store = Store()
    other_store.destination_id = "b" * 64
    archive.get = lambda _: pytest.fail("must reject before reading raw archive")
    with pytest.raises(ValidationError, match="another database"):
        AuxiliaryStatisticsCollector(tmp_path, Client(None), archive, other_store, fail_clock)
    assert not other_store.put_calls


def test_corrupt_or_deleted_destination_guard_blocks_http_and_clock_without_rebinding(tmp_path):
    client = Client(current())
    collector, archive, store = service(tmp_path, client, clock=fail_clock)
    guard = collector.directory / ".destination.json"
    original = guard.read_bytes()
    guard.write_bytes(b'{"schema_version":true,"destination_id":"broken"}')
    with pytest.raises(StorageError, match="guard"):
        collector.collect_current(INSTRUMENT)
    assert client.calls == [] and not store.put_calls and archive.values == {}
    guard.write_bytes(original)
    guard.unlink()
    with pytest.raises(StorageError, match="missing"):
        collector.collect_current(INSTRUMENT)
    assert not guard.exists() and client.calls == []
