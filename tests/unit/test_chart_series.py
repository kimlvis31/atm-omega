"""Display jobs use common producers, exact closed bars and worker-owned reads."""

import threading
from dataclasses import replace
from decimal import Decimal

import pytest

from omega.analysis.catalog import create_features
from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.chart_series import ChartSeriesService
from omega.runtime.service import CoreService

MINUTE = 60000
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
SPEC = {
    "instrument": INSTRUMENT.key,
    "start_ms": 0,
    "end_ms": 60 * MINUTE,
    "period_minutes": 1,
    "kind": "DMIxADX",
    "nSamples": 3,
}


def candles(count=60):
    return tuple(
        Candle(
            INSTRUMENT,
            minute * MINUTE,
            (minute + 1) * MINUTE,
            Decimal(100 + minute % 7),
            Decimal(110 + minute % 7),
            Decimal(90 + minute % 7),
            Decimal(102 + minute % 7),
            Decimal(10),
        )
        for minute in range(count)
    )


class Source:
    def __init__(self, rows=None):
        self.rows = candles() if rows is None else rows
        self.calls, self.closes = [], []

    def read(self, keys, start, end, *, limit, as_of_ms):
        self.calls.append((threading.get_ident(), keys, start, end, limit, as_of_ms))
        return tuple(row for row in self.rows if start <= row.open_time_ms < end)

    def close(self):
        self.closes.append(threading.get_ident())


def terminal(service, identity="one"):
    with service.condition:
        assert service.condition.wait_for(
            lambda: service.jobs[identity]["state"] not in {"queued", "running"}, timeout=5
        )
    return service.status(identity)


@pytest.mark.parametrize("period,intervalID", [(1, 0), (3, 1), (5, 2), (15, 3)])
def test_values_match_common_producer_and_retain_actual_display_knowledge(period, intervalID):
    source, owners = Source(), []

    def factory():
        owners.append(threading.get_ident())
        return source

    clock = [61 * MINUTE]
    service = ChartSeriesService(factory, clock_ms=lambda: clock[0])
    specification = {**SPEC, "period_minutes": period}
    try:
        service.submit("one", specification)
        row = terminal(service)
        assert row["state"] == "complete", row
        result = row["result"]
        graph = create_features(
            INSTRUMENT,
            (f"{intervalID}_DMIxADX_0_DMIxADX",),
            {
                "momentum": [
                    {
                        "interval_id": intervalID,
                        "analysis_code": "DMIxADX",
                        "nSamples": 3,
                        "line_index": 0,
                    }
                ]
            },
        )
        expected = []
        for candle in source.rows:
            output = graph.update(candle)
            if output[f"CLOSED_{intervalID}"]:
                expected.append(output[f"{intervalID}_DMIxADX_0_DMIxADX"])
        assert [value["DMIxADX"] for value in result["rows"]] == expected
        assert len(result["rows"]) == 60 // period
        assert result["knowledge_mode"] == "retrospective_display"
        assert result["as_of_ms"] == result["produced_at_ms"] == clock[0]
        assert all(
            value["available_at_ms"] == clock[0]
            and value["close_time_ms"] == value["open_time_ms"] + period * MINUTE
            for value in result["rows"]
        )
        assert owners == source.closes and owners[0] != threading.get_ident()
        assert all(call[-1] == clock[0] for call in source.calls)
        assert service.submit("one", specification) == row
        assert len(source.calls) == 1
        row["result"]["rows"].clear()
        assert service.status("one")["result"]["rows"]
    finally:
        service.close()


def test_gap_resets_common_kernel_without_manufacturing_missing_values():
    original = candles()
    source = Source(original[:20] + original[23:])
    service = ChartSeriesService(lambda: source, clock_ms=lambda: 61 * MINUTE)
    try:
        service.submit("one", SPEC)
        result = terminal(service)["result"]
        assert result["quality"]["missing_minutes"] == 3
        assert result["quality"]["reset_count"] == 1
        assert all(
            value["open_time_ms"] not in {20 * MINUTE, 21 * MINUTE, 22 * MINUTE}
            for value in result["rows"]
        )
        assert (
            next(value for value in result["rows"] if value["open_time_ms"] == 23 * MINUTE)[
                "DMIxADX"
            ]
            is None
        )
    finally:
        service.close()


def test_chunk_boundaries_preserve_one_graph_and_one_fixed_cutoff():
    source = Source(candles(1100))
    service = ChartSeriesService(lambda: source, clock_ms=lambda: 1101 * MINUTE)
    specification = {**SPEC, "end_ms": 1100 * MINUTE, "period_minutes": 5}
    try:
        service.submit("one", specification)
        row = terminal(service)
        assert row["state"] == "complete", row
        assert [(call[2] // MINUTE, call[3] // MINUTE) for call in source.calls] == [
            (0, 512),
            (512, 1024),
            (1024, 1100),
        ]
        assert {call[-1] for call in source.calls} == {1101 * MINUTE}
        result = row["result"]
        assert result["quality"]["reset_count"] == 0
        assert len(result["rows"]) == 220
        graph = create_features(
            INSTRUMENT,
            ("2_DMIxADX_0_DMIxADX",),
            {"momentum": [{"interval_id": 2, "analysis_code": "DMIxADX", "nSamples": 3}]},
        )
        expected = []
        for candle in source.rows:
            output = graph.update(candle)
            if output["CLOSED_2"]:
                expected.append(output["2_DMIxADX_0_DMIxADX"])
        assert [value["DMIxADX"] for value in result["rows"]] == expected
    finally:
        service.close()


@pytest.mark.parametrize(
    "change",
    [
        {"end_ms": 62 * MINUTE},
        {"start_ms": True},
        {"start_ms": 1},
        {"end_ms": 10001 * MINUTE},
        {"period_minutes": 4320},
        {"period_minutes": True},
        {"nSamples": 1},
        {"nSamples": 2001},
        {"kind": "MST"},
        {"instrument": "binance:spot:../USDT"},
        {"extra": 1},
    ],
)
def test_invalid_specs_never_open_the_source(change):
    calls = []
    service = ChartSeriesService(lambda: calls.append(1), clock_ms=lambda: 61 * MINUTE)
    try:
        with pytest.raises(ValidationError):
            service.submit("one", {**SPEC, **change})
        assert calls == [] and service.thread is None
    finally:
        service.close()


def test_conflicting_id_and_nonfinite_or_mismatched_sources_fail_without_partial_results():
    source = Source(
        (replace(candles()[0], instrument=Instrument("binance", Market.SPOT, "ETHUSDT")),)
    )
    service = ChartSeriesService(lambda: source, clock_ms=lambda: 61 * MINUTE)
    try:
        service.submit("one", SPEC)
        failed = terminal(service)
        assert failed["state"] == "failed" and failed["result"] is None
        assert failed["error"] == "ProtocolError" and source.closes
        with pytest.raises(ValidationError):
            service.submit("one", {**SPEC, "nSamples": 4})
    finally:
        service.close()


def test_close_fences_a_late_read_and_queue_capacity():
    entered, release = threading.Event(), threading.Event()
    source = Source()
    original = source.read

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(4)
        return original(*args, **kwargs)

    source.read = blocked
    service = ChartSeriesService(lambda: source, clock_ms=lambda: 61 * MINUTE)
    service.submit("one", SPEC)
    assert entered.wait(2)
    for index in range(8):
        service.submit(f"queued_{index}", SPEC)
    with pytest.raises(ValidationError):
        service.submit("overflow", SPEC)
    closer = threading.Thread(target=service.close)
    closer.start()
    with service.condition:
        assert service.condition.wait_for(lambda: service.closed, timeout=2)
    release.set()
    closer.join(3)
    assert not closer.is_alive() and not service.thread.is_alive()
    assert service.status("one")["state"] == "cancelled"
    assert service.status("one")["result"] is None
    assert len(source.closes) == 1
    with pytest.raises(StorageError):
        service.submit("after_close", SPEC)


def test_memory_expiry_preserves_request_identity_without_recalculation(monkeypatch):
    service = ChartSeriesService(Source, clock_ms=lambda: 61 * MINUTE, maximum_cached_rows=10000)
    calls = []
    monkeypatch.setattr(
        service, "_calculate", lambda specification: (calls.append(1), {"rows": [{}] * 6000})[1]
    )
    try:
        service.submit("one", SPEC)
        terminal(service)
        service.submit("two", SPEC)
        terminal(service, "two")
        assert service.status("one")["state"] == "expired"
        assert service.submit("one", SPEC)["result"] is None
        assert calls == [1, 1]
    finally:
        service.close()


def test_owner_routes_validate_envelopes_and_close_the_actual_worker(tmp_path):
    source = Source()
    worker = ChartSeriesService(lambda: source, clock_ms=lambda: 61 * MINUTE)
    core = CoreService(tmp_path, chart_series=worker)
    try:
        row = core.request(
            "POST", "/api/chart-series", core.token, {"id": "one", "parameters": SPEC}
        )
        assert row["core_epoch"] == core.state()["core"]["epoch"]
        assert core.state()["core"]["capabilities"]["chart_series"]
        terminal(worker)
        assert core.request("GET", "/api/chart-series/one", core.token)["result"]["rows"]
        for method, path, body in (
            ("POST", "/api/chart-series", {}),
            ("GET", "/api/chart-series/one?other=1", None),
        ):
            with pytest.raises(ValidationError):
                core.request(method, path, core.token, body)
        pairing = core.control.create_pairing("phone")
        device = core.request("POST", "/api/pair", "", {"code": pairing["code"], "name": "phone"})
        with pytest.raises(PermissionError):
            core.request("GET", "/api/chart-series/one", device["token"])
    finally:
        core.close()
    assert worker.closed and not worker.thread.is_alive()
