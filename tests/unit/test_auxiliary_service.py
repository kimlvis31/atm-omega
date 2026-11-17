"""Public-job orchestration with real journals and fake HTTP/archive/store boundaries."""

import json
import threading
import time
from hashlib import sha256
from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.infrastructure.auxiliary_codec import encode_observation
from omega.infrastructure.binance.auxiliary_client import AuxiliaryBatch
from omega.ports.auxiliary import AuxiliaryWriteReceipt, StoredAuxiliary
from omega.ports.raw_data import ArchivedRawResponse, RawArchiveReceipt
from omega.runtime import auxiliary_service as module
from omega.runtime.auxiliary_service import AuxiliaryService

DESTINATION = "a" * 64
INSTRUMENT = "binance:usdm:ETHUSDT"
CURRENT = {"instrument": INSTRUMENT}
TRADE = {"instrument": INSTRUMENT, "start_ms": 600000, "end_ms": 660000}
NOW = 900100


class Archive:
    def __init__(self):
        self.records = {}

    def put(self, record):
        digest = sha256(record.body).hexdigest()
        identity = sha256(
            (digest + record.channel + str(record.received_at_ms)).encode()
        ).hexdigest()
        receipt = RawArchiveReceipt(
            identity, digest, record.received_at_ms, identity not in self.records
        )
        self.records[identity] = ArchivedRawResponse(record, receipt)
        return receipt

    def get(self, identity):
        return self.records[identity]


class Client:
    def __init__(self, block=None):
        self.calls, self.block = [], block

    def current_open_interest(self, instrument):
        self.calls.append(threading.get_ident())
        if self.block:
            self.block[0].set()
            assert self.block[1].wait(3)
        return AuxiliaryBatch(
            instrument,
            "open_interest",
            (),
            b'{"symbol":"ETHUSDT","openInterest":"12.50","time":900000}',
            (),
            NOW,
            (("symbol", instrument.symbol),),
            False,
        )

    def _statistics(self, instrument, channel, payload, parameters):
        self.calls.append(threading.get_ident())
        return AuxiliaryBatch(
            instrument,
            channel,
            (),
            json.dumps([payload]).encode(),
            (),
            NOW,
            tuple((key, str(value)) for key, value in parameters.items()),
            False,
        )

    def open_interest_history(self, instrument, **parameters):
        return self._statistics(
            instrument,
            "open_interest_history",
            {
                "symbol": instrument.symbol,
                "sumOpenInterest": "12.5",
                "sumOpenInterestValue": "25",
                "timestamp": 600000,
            },
            parameters,
        )

    def long_short_ratio(self, instrument, **parameters):
        return self._statistics(
            instrument,
            "long_short_ratio",
            {
                "symbol": instrument.symbol,
                "longShortRatio": "2",
                "longAccount": ".6667",
                "shortAccount": ".3333",
                "timestamp": 600000,
            },
            parameters,
        )


class Store:
    destination_id = DESTINATION

    def __init__(self, lost_ack=False):
        self.records, self.calls, self.closes = {}, [], []
        self.lost_ack, self.fail_close = lost_ack, False

    def put(self, instrument, observation, *, expected_previous=None):
        self.calls.append(threading.get_ident())
        key = (instrument.key, observation.channel, observation.source, observation.window_start_ms)
        digest = sha256(encode_observation(instrument, observation)).hexdigest()
        if key not in self.records:
            assert expected_previous is None
            self.records[key] = StoredAuxiliary(instrument, observation, 1, digest, None)
        if self.lost_ack:
            self.lost_ack = False
            raise StorageError("synthetic acknowledged-lost commit")
        return AuxiliaryWriteReceipt(1, digest, False)

    def get(self, instrument, channel, source, opening, *, as_of_ms):
        self.calls.append(threading.get_ident())
        result = self.records.get((instrument.key, channel, source, opening))
        return result if result and result.observation.available_at_ms <= as_of_ms else None

    def close(self):
        self.closes.append(threading.get_ident())
        if self.fail_close:
            raise StorageError("synthetic close failure")


def create(tmp_path, *, client=None, store=None, archive=None):
    client, store, archive = client or Client(), store or Store(), archive or Archive()
    owners = []

    def factory(plan):
        owners.append((threading.get_ident(), plan))
        return client, archive, store

    service = AuxiliaryService(
        tmp_path, destination_id=DESTINATION, resource_factory=factory, clock_ms=lambda: NOW
    )
    return service, client, store, archive, owners


def finished(service, identity="one"):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        result = service.status(identity)
        if result["state"] not in {"queued", "running", "cancelling"}:
            return result
        threading.Event().wait(0.005)
    pytest.fail("bounded auxiliary job did not finish")


def shutdown(service):
    result = service.close()
    if service.thread:
        service.thread.join(3)
    assert not service.snapshot()["worker_alive"]
    return result


def repair_parameters(parent):
    return {
        "instrument": INSTRUMENT,
        "parent_job_id": parent["id"],
        "parent_execution_id": parent["execution_id"],
        "open_time_ms": TRADE["start_ms"],
    }


def partial_parent(service):
    # Exercise the service owner and durable row validation without public I/O.
    service._execute = lambda row, stop: {"complete": False, "reason": "incomplete_coverage"}
    service.submit("parent", "trade_flow", TRADE)
    return finished(service, "parent")


def test_explicit_repair_submission_is_idempotent_and_survives_service_restart(tmp_path):
    service, *_ = create(tmp_path)
    parent = partial_parent(service)
    parameters = repair_parameters(parent)
    try:
        repair = service.submit("repair", "trade_flow_repair", parameters)
        assert repair["parameters"]["limit"] == 500
        assert repair["parameters"]["max_pages"] == 20
        result = finished(service, "repair")
        assert result["state"] == "partial"
        assert service.submit("repair", "trade_flow_repair", parameters) == result
        assert service.status("parent") == parent
    finally:
        shutdown(service)
    restored, *_ = create(tmp_path)
    try:
        assert restored.status("repair")["parameters"] == repair["parameters"]
        assert restored.snapshot()["worker_alive"] is False
        assert restored.submit("repair", "trade_flow_repair", parameters) == result
    finally:
        shutdown(restored)


@pytest.mark.parametrize(
    "change",
    [
        {"parent_job_id": "absent"},
        {"parent_execution_id": "f" * 32},
        {"instrument": "binance:spot:ETHUSDT"},
        {"open_time_ms": TRADE["end_ms"]},
        {"open_time_ms": True},
        {"open_time_ms": TRADE["start_ms"] + 1},
        {"max_pages": 0},
        {"limit": True},
        {"expected_previous": "a" * 64},
    ],
)
def test_repair_requires_the_current_parent_and_exact_bounded_minute(tmp_path, change):
    service, *_ = create(tmp_path)
    parent = partial_parent(service)
    try:
        with pytest.raises(ValidationError):
            service.submit("repair", "trade_flow_repair", {**repair_parameters(parent), **change})
        assert "repair" not in service.jobs
        assert service.status("parent") == parent
    finally:
        shutdown(service)


def test_actual_statistics_commit_is_pinned_and_all_resources_belong_to_worker(tmp_path):
    service, client, store, archive, owners = create(tmp_path)
    queued = service.submit("one", "current_oi", CURRENT)
    result = finished(service)
    assert queued["state"] == "queued" and result["state"] == "completed"
    assert result["archive_id"] in archive.records
    assert result["result"]["rows"][0]["revision"] == 1
    owner = owners[0][0]
    assert owner != threading.get_ident() and set(client.calls + store.calls + store.closes) == {
        owner
    }
    result["parameters"].clear()
    assert service.status("one")["parameters"] == CURRENT
    assert service.submit("one", "current_oi", CURRENT)["execution_id"] == queued["execution_id"]
    assert len(client.calls) == 1
    shutdown(service)


def test_lost_commit_ack_resumes_same_archive_without_new_http_or_normalization_clock(tmp_path):
    service, client, store, _, _ = create(tmp_path, store=Store(lost_ack=True))
    old = service.submit("one", "current_oi", CURRENT)
    failed = finished(service)
    assert failed["state"] == "unknown" and failed["archive_id"] and len(store.records) == 1
    service.clock_ms = lambda: pytest.fail("frozen statistics recovery must not sample a clock")
    resumed = service.resume("one", old["execution_id"])
    assert resumed["execution_id"] != old["execution_id"]
    assert finished(service)["state"] == "completed" and len(client.calls) == 1
    assert len(store.records) == 1
    with pytest.raises(ValidationError, match="stale"):
        service.cancel("one", old["execution_id"])
    shutdown(service)


def test_queued_cancel_avoids_factory_and_running_cancel_preserves_actual_commit(tmp_path):
    events = threading.Event(), threading.Event()
    service, client, store, _, owners = create(tmp_path, client=Client(events))
    one = service.submit("one", "current_oi", CURRENT)
    assert events[0].wait(1)
    two = service.submit("two", "current_oi", CURRENT)
    cancelled = service.cancel("two", two["execution_id"])
    assert cancelled["state"] == "cancelled"
    assert service.cancel("one", one["execution_id"])["state"] == "cancelling"
    assert service.status("one")["cancel_requested"]
    events[1].set()
    completed = finished(service)
    assert completed["state"] == "completed" and completed["cancel_requested"]
    assert len(client.calls) == len(owners) == 1 and len(store.records) == 1
    shutdown(service)


def test_close_is_responsive_and_late_archive_callback_cannot_overwrite_new_owner(tmp_path):
    events = threading.Event(), threading.Event()
    service, _, store, _, _ = create(tmp_path, client=Client(events))
    service.submit("one", "current_oi", CURRENT)
    assert events[0].wait(1)
    started = time.monotonic()
    result = service.close()
    assert time.monotonic() - started < 0.5 and result["durable"] and result["worker_alive"]
    reloaded, client, _, _, owners = create(tmp_path)
    assert reloaded.status("one")["state"] == "unknown" and owners == []
    with pytest.raises(ValidationError, match="without archive"):
        reloaded.resume("one")
    saved = service.path.read_bytes()
    events[1].set()
    service.thread.join(3)
    assert service.path.read_bytes() == saved and not store.records and client.calls == []
    shutdown(reloaded)


def test_interrupted_trade_resume_is_manual_and_lock_busy_does_not_guess_success(
    tmp_path, monkeypatch
):
    entered, released = threading.Event(), threading.Event()

    class Trade:
        def __init__(self, *args, **kwargs):
            pass

        def run(self):
            entered.set()
            assert released.wait(3)
            return {"complete": True, "commit_outcome": "confirmed"}

    monkeypatch.setattr(module, "AuxiliaryCollection", Trade)
    service, _, _, _, _ = create(tmp_path)
    service.submit("one", "trade_flow", TRADE)
    assert entered.wait(1)
    service.close()
    new, _, _, _, owners = create(tmp_path)
    assert not new.thread and new.status("one")["state"] == "unknown"
    new.resume("one")
    blocked = finished(new)
    assert blocked["state"] == "unknown" and blocked["error"] == "RuntimeError" and owners == []
    released.set()
    service.thread.join(3)
    assert new.status("one")["state"] == "unknown"
    shutdown(new)


def test_unknown_statistics_without_archive_requires_new_explicit_request(tmp_path):
    service = AuxiliaryService(
        tmp_path,
        destination_id=DESTINATION,
        resource_factory=lambda _: (_ for _ in ()).throw(StorageError("private detail")),
    )
    service.submit("one", "current_oi", CURRENT)
    result = finished(service)
    assert result["error"] == "StorageError" and "private detail" not in service.path.read_text()
    with pytest.raises(ValidationError, match="without archive"):
        service.resume("one")
    shutdown(service)


@pytest.mark.parametrize(
    ("kind", "parameters"),
    [
        ("current_oi", {"instrument": "binance:spot:ETHUSDT"}),
        ("current_oi", {**CURRENT, "api_key": "forbidden"}),
        ("trade_flow", {**TRADE, "end_ms": 660001}),
        ("trade_flow", {**TRADE, "max_pages": 21}),
        ("history_oi", {**TRADE, "limit": 501}),
        ("trade_flow", {**TRADE, "end_ms": 960000}),
    ],
)
def test_invalid_or_future_plans_never_create_worker(tmp_path, kind, parameters):
    service, _, _, _, owners = create(tmp_path)
    with pytest.raises(ValidationError):
        service.submit("one", kind, parameters)
    assert owners == [] and not service.thread and service.snapshot()["jobs"] == []
    shutdown(service)


def test_id_conflicts_queue_capacity_and_detached_frozen_plan(tmp_path):
    events = threading.Event(), threading.Event()
    service, _, _, _, _ = create(tmp_path, client=Client(events))
    params = dict(CURRENT)
    service.submit("first", "current_oi", params)
    assert events[0].wait(1)
    params["instrument"] = "binance:usdm:BTCUSDT"
    with pytest.raises(ValidationError, match="conflicts"):
        service.submit("first", "current_oi", params)
    for index in range(8):
        service.submit("queue" + str(index), "current_oi", CURRENT)
    with pytest.raises(ValidationError, match="queue capacity"):
        service.submit("overflow", "current_oi", CURRENT)
    assert service.status("first")["parameters"] == CURRENT
    service.close()
    events[1].set()
    shutdown(service)


def test_atomic_submit_failure_does_not_execute_and_close_reports_failure(tmp_path, monkeypatch):
    service, _, _, _, owners = create(tmp_path)
    monkeypatch.setattr(module, "atomic_json", lambda *args: (_ for _ in ()).throw(OSError("disk")))
    with pytest.raises(StorageError):
        service.submit("one", "current_oi", CURRENT)
    assert not owners and not service.jobs and service.storage_failed
    assert not service.close()["durable"]


def test_bad_or_destination_mismatched_service_file_is_preserved_before_resources(tmp_path):
    service, _, _, _, _ = create(tmp_path)
    shutdown(service)
    saved = service.path.read_bytes()
    with pytest.raises(StorageError, match="preserved"):
        AuxiliaryService(tmp_path, destination_id="b" * 64)
    assert service.path.read_bytes() == saved
    envelope = json.loads(saved)
    envelope["data"]["schema_version"] = True
    envelope["sha256"] = sha256(module._encoded(envelope["data"])).hexdigest()
    service.path.write_bytes(module._encoded(envelope))
    corrupt = service.path.read_bytes()
    with pytest.raises(StorageError):
        create(tmp_path)
    assert service.path.read_bytes() == corrupt


@pytest.mark.parametrize(
    ("port", "dbname", "user"), [(5432, "atmEta", "postgres"), (55433, "omega_test", "postgres")]
)
def test_default_composition_rejects_legacy_destination_before_client_or_store(
    tmp_path, monkeypatch, port, dbname, user
):
    monkeypatch.setattr(
        "omega.runtime.auxiliary_resources.load_database_settings",
        lambda _: SimpleNamespace(host="127.0.0.1", port=port, dbname=dbname, user=user),
    )
    monkeypatch.setattr(
        "omega.runtime.auxiliary_resources.BinanceClient",
        lambda *args, **kwargs: pytest.fail("no client on invalid destination"),
    )
    service = AuxiliaryService(tmp_path, destination_id=DESTINATION)
    service.submit("one", "current_oi", CURRENT)
    result = finished(service)
    assert result["state"] == "unknown" and result["error"] == "ValidationError"
    shutdown(service)


def test_resource_close_failure_releases_trade_owner_lock(tmp_path, monkeypatch):
    class Trade:
        def __init__(self, *args, **kwargs):
            pass

        def run(self):
            return {"complete": False, "commit_outcome": "none"}

    monkeypatch.setattr(module, "AuxiliaryCollection", Trade)
    store = Store()
    store.fail_close = True
    service, _, _, _, owners = create(tmp_path, store=store)
    old = service.submit("one", "trade_flow", TRADE)
    assert finished(service)["state"] == "unknown"
    store.fail_close = False
    service.resume("one", old["execution_id"])
    assert finished(service)["state"] == "partial" and len(owners) == 2
    service.clock_ms = lambda: pytest.fail("a duplicate request must return its durable status")
    assert service.submit("one", "trade_flow", TRADE)["state"] == "partial"
    shutdown(service)


@pytest.mark.parametrize("kind", ["history_oi", "long_short_ratio"])
def test_native_statistics_are_single_actual_sample_jobs_not_per_minute_carries(tmp_path, kind):
    service, client, store, _, _ = create(tmp_path)
    service.submit("one", kind, {"instrument": INSTRUMENT, "start_ms": 600000, "end_ms": 900000})
    assert finished(service)["state"] == "completed" and len(client.calls) == 1
    assert len(store.records) == 1
    stored = next(iter(store.records.values())).observation
    assert stored.event_time_ms == stored.window_start_ms == 600000
    assert stored.mapping()["SOURCE_PERIOD_MS"] == 300000
    shutdown(service)


def test_queued_statistics_after_shutdown_can_resume_without_guessing_an_old_http_request(tmp_path):
    events = threading.Event(), threading.Event()
    service, _, _, _, _ = create(tmp_path, client=Client(events))
    service.submit("one", "current_oi", CURRENT)
    assert events[0].wait(1)
    service.submit("queued", "current_oi", CURRENT)
    service.close()
    events[1].set()
    service.thread.join(3)
    new, client, _, _, _ = create(tmp_path)
    queued = new.status("queued")
    assert queued["state"] == "unknown" and not queued["worker_started"]
    new.resume("queued", queued["execution_id"])
    assert finished(new, "queued")["state"] == "completed" and len(client.calls) == 1
    with pytest.raises(ValidationError, match="without archive"):
        new.resume("one")
    shutdown(new)


def test_store_destination_rebind_is_rejected_before_any_http_or_archive(tmp_path):
    store = Store()
    store.destination_id = "b" * 64
    service, client, _, archive, _ = create(tmp_path, store=store)
    service.submit("one", "current_oi", CURRENT)
    result = finished(service)
    assert result["state"] == "unknown" and client.calls == [] and archive.records == {}
    assert len(store.closes) == 1
    shutdown(service)


def test_archive_before_statistics_journal_crash_gap_never_reissues_http(tmp_path, monkeypatch):
    original = module.atomic_json
    service, client, _, _, _ = create(tmp_path)

    def fail_after_archive(*args, **kwargs):
        if str(args[0]).endswith(".destination.json"):
            return original(*args, **kwargs)
        if str(args[0]).endswith("auxiliary-service.json"):
            return original(*args, **kwargs)
        raise OSError("synthetic statistics journal failure after archive callback")

    monkeypatch.setattr("omega.runtime.auxiliary_statistics.atomic_json", fail_after_archive)
    service.submit("one", "current_oi", CURRENT)
    first = finished(service)
    assert first["state"] == "unknown" and first["archive_id"] and len(client.calls) == 1
    monkeypatch.setattr("omega.runtime.auxiliary_statistics.atomic_json", original)
    service.resume("one", first["execution_id"])
    recovered = finished(service)
    assert recovered["state"] == "completed" and len(client.calls) == 1
    assert recovered["result"]["recovered_missing_journal"] is True
    shutdown(service)
