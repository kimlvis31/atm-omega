"""Injected WS/REST/archive/store plus a real local journal; public network0."""

import asyncio
import json
import threading
import time
from dataclasses import replace
from hashlib import sha256

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_codec import encode_observation
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.transport import HttpResponse
from omega.ports.auxiliary import AuxiliaryWriteReceipt, StoredAuxiliary
from omega.ports.raw_data import ArchivedRawResponse, RawArchiveReceipt
from omega.runtime.depth_collection import DepthCollection, depth_url


def instrument(market=Market.USDM):
    return Instrument("binance", market, "ETHUSDT")


def config(value=None):
    return [{"instrument": value or instrument(), "price_precision": 2, "quote_precision": 2}]


def wire(market, identifier, when, *, previous=None):
    row = {
        "e": "depthUpdate",
        "s": "ETHUSDT",
        "E": when,
        "U": 10 if identifier == 11 else identifier,
        "u": identifier,
        "b": [],
        "a": [],
    }
    if market is Market.USDM:
        row.update(T=when, pu=identifier - 1 if previous is None else previous)
    return json.dumps({"stream": "ethusdt@depth", "data": row}).encode()


class Clock:
    value = 0

    def __call__(self):
        return self.value


class Harness:
    def __init__(self, market=Market.USDM):
        self.market, self.clock = market, Clock()
        self.rows, self.raw, self.log, self.requests = {}, {}, [], []
        self.contexts, self.closed_threads, self.threads = [], [], []
        self.http_threads, self.loop_ticks = [], 0
        self.events = [
            (1000, wire(market, 11, 1000)),
            (59999, wire(market, 12, 59999)),
            (60000, wire(market, 13, 60000)),
        ]
        self.fail_archive, self.lose_store_ack = False, False
        self.get_started = self.release_get = None
        self.narrow = False

    def factory(self, value):
        self.threads.append(threading.get_ident())
        harness = self

        class Transport:
            def send(self, request):
                harness.http_threads.append(threading.get_ident())
                harness.requests.append(request)
                assert request.method == "GET" and "signature" not in request.url
                assert not any("key" in key.lower() for key in request.headers)
                if harness.get_started is not None:
                    harness.get_started.set()
                    assert harness.release_get.wait(3)
                data = {
                    "lastUpdateId": 10,
                    "bids": [["99.9", "1"], ["99" if harness.narrow else "95", "1"]],
                    "asks": [["100.1", "1"], ["101" if harness.narrow else "105", "1"]],
                }
                return HttpResponse(
                    200,
                    json.dumps(data).encode(),
                    {"Content-Type": "application/json", "X-MBX-USED-WEIGHT-1M": "50"},
                )

            def close(self):
                harness.closed_threads.append(threading.get_ident())

        class Archive:
            def put(self, record):
                harness.threads.append(threading.get_ident())
                if harness.fail_archive:
                    raise OSError("synthetic archive failure")
                identity = sha256(
                    record.body + repr(record.request_parameters).encode()
                ).hexdigest()
                receipt = RawArchiveReceipt(
                    identity,
                    sha256(record.body).hexdigest(),
                    record.received_at_ms,
                    identity not in harness.raw,
                )
                harness.raw[identity] = ArchivedRawResponse(record, receipt)
                harness.log.append(("archive", record.channel))
                return receipt

            def get(self, identity):
                harness.threads.append(threading.get_ident())
                return harness.raw[identity]

        class Store:
            destination_id = "a" * 64

            def get(self, found, channel, source, opened, *, as_of_ms):
                harness.threads.append(threading.get_ident())
                item = harness.rows.get((found, channel, source, opened))
                return (
                    item
                    if item is not None and item.observation.available_at_ms <= as_of_ms
                    else None
                )

            def put(self, found, observation, *, expected_previous):
                harness.threads.append(threading.get_ident())
                key = found, observation.channel, observation.source, observation.window_start_ms
                assert key not in harness.rows and expected_previous is None
                digest = sha256(encode_observation(found, observation)).hexdigest()
                harness.rows[key] = StoredAuxiliary(found, observation, 1, digest, None)
                harness.log.append(("put", observation.quality))
                if harness.lose_store_ack:
                    harness.lose_store_ack = False
                    raise StorageError("synthetic commit ACK loss")
                return AuxiliaryWriteReceipt(1, digest, True)

            def close(self):
                harness.closed_threads.append(threading.get_ident())

        origin = (
            "https://api.binance.com" if value.market is Market.SPOT else "https://fapi.binance.com"
        )
        budget = RequestBudget(
            (Window("binance-ip", 600, 60),), clock=lambda: harness.clock() / 1000
        )
        return (
            BinanceClient(origin, Transport(), budget=budget, clock_ms=self.clock),
            Archive(),
            Store(),
        )

    def connect(self, value):
        harness = self

        class Socket:
            async def __aenter__(self):
                harness.contexts.append(self)
                self.closed = False

                async def incoming_protocol():
                    while True:
                        harness.loop_ticks += 1
                        await asyncio.sleep(0.005)

                self.protocol = asyncio.create_task(incoming_protocol())
                return self

            async def __aexit__(self, *args):
                self.closed = True
                self.protocol.cancel()
                try:
                    await self.protocol
                except asyncio.CancelledError:
                    pass

            async def recv(self, *, decode):
                assert decode is False
                if harness.events:
                    when, raw = harness.events.pop(0)
                    harness.clock.value = when
                    await asyncio.sleep(0)
                    return raw
                await asyncio.Future()

        return Socket()


def wait_for(predicate):
    deadline = time.monotonic() + 5
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("bounded fake depth worker did not reach its expected state")
        time.sleep(0.005)


def service(tmp_path, harness, **kwargs):
    return DepthCollection(
        tmp_path, harness.factory, clock_ms=harness.clock, connect=harness.connect, **kwargs
    )


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_actual_worker_journal_public_shapes_raw_before_commit_and_owner_thread(tmp_path, market):
    harness = Harness(market)
    owner = service(tmp_path, harness)
    assert not tmp_path.joinpath("state").exists() and not harness.requests
    try:
        owner.start(config(instrument(market)))
        wait_for(lambda: owner.snapshot()["instruments"][0]["cursor_ms"] == 60000)
        stored = next(iter(harness.rows.values()))
        assert stored.observation.quality == "observed" and stored.observation.coverage == 1
        assert stored.observation.source == instrument(market).key + ":binance_depth_ws_v1"
        assert (
            stored.observation.event_time_ms == 59999
            and stored.observation.available_at_ms == 60000
        )
        assert len(harness.requests) == 1
        assert ("/api/v3/depth" if market is Market.SPOT else "/fapi/v1/depth") in harness.requests[
            0
        ].url
        assert "limit=1000" in harness.requests[0].url
        assert all(
            kind == "archive" for kind, _ in harness.log[: harness.log.index(("put", "observed"))]
        )
        assert {item.record.channel for item in harness.raw.values()} == {
            "depth_diff",
            "depth_snapshot",
        }
        assert any(item.record.body == wire(market, 12, 59999) for item in harness.raw.values())
    finally:
        owner.close()
    assert all(context.closed for context in harness.contexts)
    assert len(set(harness.threads + harness.closed_threads)) == 1
    assert next(iter(harness.threads)) != threading.get_ident()
    assert owner.snapshot()["closed"] and not owner.snapshot()["running"]


def test_insufficient_spatial_coverage_stays_partial_not_zero_complete(tmp_path):
    harness = Harness()
    harness.narrow = True
    owner = service(tmp_path, harness)
    try:
        owner.start(config())
        wait_for(lambda: bool(harness.rows))
        observation = next(iter(harness.rows.values())).observation
        assert observation.quality == "partial" and observation.coverage < 1
        assert "DEPTH_BIDS5" not in observation.mapping()
    finally:
        owner.close()


def test_commit_ack_loss_stops_new_gets_and_explicit_resume_reuses_original_input(tmp_path):
    harness = Harness()
    harness.lose_store_ack = True
    owner = service(tmp_path, harness)
    try:
        owner.start(config())
        wait_for(lambda: not owner.snapshot()["running"])
        assert owner.snapshot()["instruments"][0]["state"] == "unknown"
        original = next(iter(harness.rows.values()))
        assert len(harness.requests) == 1
    finally:
        owner.close()
    blocked = service(tmp_path, harness)
    try:
        blocked.start(config())
        wait_for(lambda: not blocked.snapshot()["running"])
        assert blocked.snapshot()["instruments"][0]["state"] == "unknown"
        assert len(harness.requests) == 1
    finally:
        blocked.close()
    restored = service(tmp_path, harness)
    try:
        restored.start(config(), resume=True)
        wait_for(lambda: restored.snapshot()["instruments"][0]["cursor_ms"] == 60000)
        assert next(iter(harness.rows.values())) == original
        assert len(harness.requests) == 1 and len(harness.rows) == 1
    finally:
        restored.close()


def test_archive_failure_is_unknown_no_http_retry_or_normalized_publish(tmp_path):
    harness = Harness()
    harness.fail_archive = True
    owner = service(tmp_path, harness)
    try:
        owner.start(config())
        wait_for(lambda: not owner.snapshot()["running"])
        assert owner.snapshot()["instruments"][0]["state"] == "unknown"
        assert not harness.requests and not harness.rows
    finally:
        owner.close()


def test_gap_is_explicit_backoff_and_stop_does_not_start_another_request(tmp_path):
    harness = Harness()
    harness.events[1] = 59999, wire(Market.USDM, 12, 59999, previous=100)
    owner = service(tmp_path, harness)
    try:
        owner.start(config())
        wait_for(lambda: owner.snapshot()["instruments"][0]["state"] == "backoff")
        assert len(harness.requests) == 1 and not harness.rows
        assert owner.snapshot()["instruments"][0]["reason"] == "ProtocolError"
    finally:
        owner.close()
    assert len(harness.requests) == 1


def test_stop_during_snapshot_get_prevents_late_archive_save_and_db_publish(tmp_path):
    harness = Harness()
    harness.get_started, harness.release_get = threading.Event(), threading.Event()
    owner = service(tmp_path, harness)
    try:
        owner.start(config())
        assert harness.get_started.wait(3)
        wait_for(lambda: harness.loop_ticks >= 3)
        assert not harness.closed_threads
        raw_count = len(harness.raw)
        owner.request_stop()
        harness.release_get.set()
        wait_for(lambda: not owner.snapshot()["running"])
        assert not harness.rows and len(harness.raw) == raw_count
    finally:
        harness.release_get.set()
        owner.close()


def test_raw_quota_stops_honestly_and_corrupt_state_is_preserved(tmp_path):
    harness = Harness()
    owner = service(tmp_path, harness, max_raw_bytes=1)
    try:
        owner.start(config())
        wait_for(lambda: not owner.snapshot()["running"])
        assert owner.snapshot()["instruments"][0]["state"] == "unknown"
        assert not harness.requests and not harness.rows
    finally:
        owner.close()
    path = owner._path(instrument())
    path.write_bytes(b'{"corrupt":true}')
    restarted = service(tmp_path, harness)
    try:
        restarted.start(config(), resume=True)
        wait_for(lambda: not restarted.snapshot()["running"])
        assert restarted.snapshot()["instruments"][0]["state"] == "unknown"
        assert path.read_bytes() == b'{"corrupt":true}' and not harness.requests
    finally:
        restarted.close()


def test_config_bound_security_and_fixed_market_routes(tmp_path):
    harness = Harness()
    owner = service(tmp_path, harness)
    with pytest.raises(ValidationError):
        owner.start(config() * 9)
    with pytest.raises(ValidationError):
        owner.start([{**config()[0], "price_precision": True}])
    assert not tmp_path.joinpath("state").exists()
    assert depth_url(instrument()).startswith("wss://fstream.binance.com/public/stream?")
    assert depth_url(instrument(Market.SPOT)).startswith("wss://stream.binance.com:443/stream?")
    owner.close()
    with pytest.raises(ValidationError):
        owner.start(config())


def test_destination_rebinding_is_refused_before_http(tmp_path):
    harness = Harness()
    owner = service(tmp_path, harness)
    try:
        owner.start(config())
        wait_for(lambda: bool(harness.rows))
    finally:
        owner.close()
    old_factory = harness.factory

    def wrong_destination(value):
        client, archive, store = old_factory(value)
        store.destination_id = "b" * 64
        return client, archive, store

    rebound = DepthCollection(
        tmp_path, wrong_destination, clock_ms=harness.clock, connect=harness.connect
    )
    try:
        rebound.start(config(), resume=True)
        wait_for(lambda: not rebound.snapshot()["running"])
        assert rebound.snapshot()["instruments"][0]["state"] == "unknown"
        assert len(harness.requests) == 1
    finally:
        rebound.close()


def test_native_raw_config_and_nonblocking_stop_during_http(tmp_path):
    harness = Harness()
    harness.get_started, harness.release_get = threading.Event(), threading.Event()
    owner = service(tmp_path, harness)
    try:
        owner.start([{**config()[0], "instrument": instrument().key}])
        assert harness.get_started.wait(3)
        started = time.monotonic()
        result = owner.request_stop()
        assert time.monotonic() - started < 0.5 and result["running"]
        wait_for(lambda: owner.snapshot()["instruments"][0]["state"] == "stopping_http")
        assert not harness.closed_threads
        owner.request_stop()
        assert not harness.closed_threads
        harness.release_get.set()
    finally:
        harness.release_get.set()
        owner.close()
    assert not harness.rows and all(context.closed for context in harness.contexts)


def test_equal_duplicate_diff_and_original_receipt_clock_remain_honest(tmp_path):
    harness = Harness()
    harness.events.insert(1, harness.events[0])
    harness.events[2] = 60400, wire(Market.USDM, 12, 59999)
    harness.events[3] = 60400, wire(Market.USDM, 13, 60000)
    owner = service(tmp_path, harness)
    try:
        owner.start(config())
        wait_for(lambda: bool(harness.rows))
        observed = next(iter(harness.rows.values())).observation
        assert observed.quality == "observed" and observed.event_time_ms == 59999
        assert observed.mapping()["DEPTH_RECEIVED_AT_MS"] == 60400
        assert observed.available_at_ms == 60400 and len(harness.requests) == 1
    finally:
        owner.close()


def test_behind_clock_waits_before_any_minute_publication(tmp_path):
    harness = Harness()
    harness.events = [harness.events[0], (59600, wire(Market.USDM, 12, 59999))]
    owner = service(tmp_path, harness)
    try:
        owner.start(config())
        wait_for(
            lambda: any(
                raw.record.body == wire(Market.USDM, 12, 59999)
                for raw in list(harness.raw.values())
            )
        )
        assert not harness.rows
        harness.clock.value = 60001
        wait_for(lambda: bool(harness.rows))
        observed = next(iter(harness.rows.values())).observation
        assert observed.event_time_ms == 59999 and observed.available_at_ms == 60001
        assert observed.mapping()["DEPTH_RECEIVED_AT_MS"] == 59600
    finally:
        owner.close()


def test_raw_body_tamper_blocks_fixed_resume_without_new_get(tmp_path):
    harness = Harness()
    harness.lose_store_ack = True
    first = service(tmp_path, harness)
    try:
        first.start(config())
        wait_for(lambda: not first.snapshot()["running"])
    finally:
        first.close()
    identity = next(iter(harness.raw))
    archived = harness.raw[identity]
    harness.raw[identity] = replace(archived, record=replace(archived.record, body=b"tampered"))
    restarted = service(tmp_path, harness)
    try:
        restarted.start(config(), resume=True)
        wait_for(lambda: not restarted.snapshot()["running"])
        assert restarted.snapshot()["instruments"][0]["state"] == "unknown"
        assert len(harness.requests) == 1 and len(harness.rows) == 1
    finally:
        restarted.close()


def test_minute_cap_never_overwrites_prior_commit_and_close_fault_is_reported(tmp_path):
    harness = Harness()
    harness.events.extend(
        [(119999, wire(Market.USDM, 14, 119999)), (120000, wire(Market.USDM, 15, 120000))]
    )
    owner = service(tmp_path, harness, max_minutes=1)
    try:
        owner.start(config())
        wait_for(lambda: not owner.snapshot()["running"])
        assert len(harness.rows) == 1 and owner.snapshot()["instruments"][0]["cursor_ms"] == 60000
        assert owner.snapshot()["instruments"][0]["state"] == "unknown"
    finally:
        owner.close()


def test_resource_close_error_keeps_other_closes_and_owner_release(tmp_path):
    harness = Harness()
    original = harness.factory

    def resources(value):
        client, archive, store = original(value)
        close = store.close

        def fail():
            close()
            raise OSError("synthetic store close failure")

        store.close = fail
        return client, archive, store

    owner = DepthCollection(tmp_path, resources, clock_ms=harness.clock, connect=harness.connect)
    owner.start(config())
    wait_for(lambda: bool(harness.rows))
    owner.request_stop()
    wait_for(lambda: not owner.snapshot()["running"])
    with pytest.raises(StorageError, match="closed"):
        owner.close()
    assert len(harness.closed_threads) == 2
    assert owner.snapshot()["close_errors"] == ["resource_close_failed"]
    recovered = service(tmp_path, harness)
    try:
        recovered.start(config())
        wait_for(
            lambda: (
                bool(harness.contexts)
                and recovered.snapshot()["instruments"][0]["state"] == "connecting"
            )
        )
        assert recovered.snapshot()["running"]
    finally:
        recovered.close()
