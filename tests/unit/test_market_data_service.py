"""No real DB/network: operation durability, fast cached views and secret boundaries."""

import json
import threading
import time
from copy import deepcopy
from decimal import Decimal

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.domain.execution_rules import DecimalRange, ExecutionRulesSnapshot, NotionalRule
from omega.domain.models import Instrument, Market
from omega.infrastructure.market_data import MarketDataBackend
from omega.runtime.market_data_service import MarketDataService, empty_detail

KEY = "binance:usdm:BTCUSDT"


def test_large_catalog_keeps_full_rules_only_in_selected_detail_within_gui_response_budget(
    tmp_path,
):
    backend = Backend()
    service = MarketDataService(
        tmp_path, backend=backend, collectors=Collectors(), clock_ms=lambda: 900100
    )
    try:
        catalog = {}
        keys = [KEY, *(f"binance:usdm:TEST{index}USDT" for index in range(3000))]
        for key in keys:
            instrument = Instrument("binance", Market.USDM, key.rsplit(":", 1)[1])
            rules = ExecutionRulesSnapshot(
                instrument,
                900100,
                "binance:usdm:exchangeInfo",
                "TRADING",
                DecimalRange(Decimal(".001"), Decimal(1000), Decimal(".001")),
                None,
                DecimalRange(step=Decimal(".01")),
                notionals=(NotionalRule(minimum=Decimal(10)),),
                unsupported_filters=("MAX_NUM_ALGO_ORDERS", "MAX_NUM_ORDERS", "PERCENT_PRICE"),
                reductions_allowed=True,
            )
            catalog[key] = {"status": "TRADING", "metadata": {"execution_rules": rules.document()}}
        service._install_catalog("usdm", catalog)
        service._refresh("usdm", KEY)
        snapshot = service.snapshot("usdm", KEY)
        assert len(snapshot["instruments"]) == 3001
        assert all("execution_rules" not in row["metadata"] for row in snapshot["instruments"])
        assert snapshot["selected"]["metadata"]["execution_rules"]["instrument"] == KEY
        assert len(json.dumps(snapshot).encode()) < 4 * 1024 * 1024
        assert (
            service.execution_rules(KEY).content_hash
            == snapshot["selected"]["metadata"]["execution_rules"]["content_hash"]
        )
    finally:
        service.close()


class Backend:
    def __init__(self):
        self.calls = []
        self.block = threading.Event()
        self.block.set()
        self.entered = threading.Event()
        self.fail_refresh = False

    def refresh(self, market, selected):
        self.calls.append(("refresh", market, selected))
        self.entered.set()
        self.block.wait(2)
        if self.fail_refresh:
            raise StorageError("DB unavailable")
        return {
            "database": {"state": "ready", "name": "omega", "size_bytes": 1000},
            "instruments": [
                {
                    "instrument": f"binance:{market}:BTCUSDT",
                    "symbol": "BTCUSDT",
                    "market": market,
                    "status": "observed",
                    "rows": 2,
                    "gap_rows": 1,
                    "availability": 2 / 3,
                    "gap_rate": 1 / 3,
                    "dummy_rate": None,
                }
            ],
            "selected": empty_detail(selected) if selected else None,
            "last_fetched": 180_000,
        }

    def catalog(self, market):
        self.calls.append(("catalog", market))
        self.block.wait(2)
        return {
            f"binance:{market}:BTCUSDT": {
                "status": "TRADING",
                "metadata": {"base_asset": "BTC", "quote_asset": "USDT"},
            }
        }

    def refetch(self, key, lookback, progress, stop):
        self.calls.append(("refetch", key, lookback))
        progress(0.5)
        return {"inserted_rows": 1, "remaining_ranges": 0, "observed_only": True}

    def import_peer(self, key, parameters, lookback, progress, stop):
        self.calls.append(("peer", key, lookback))
        if parameters["password"] == "fail-secret":
            raise RuntimeError("fail-secret must never reach a response")
        return {"inserted_rows": 1, "remaining_ranges": 0, "observed_only": True}


class Collectors:
    def __init__(self):
        self.calls, self.plans, self.active = [], [], set()

    def configure(self, market, instruments, lookback, historical, *, starts=None):
        self.calls.append((market, list(instruments), lookback, historical))
        self.plans.append(starts)
        if instruments:
            self.active.add(market)
        else:
            self.active.discard(market)

    def running(self, market):
        return market in self.active

    def close(self):
        self.active.clear()


@pytest.fixture
def service(tmp_path):
    backend, collectors = Backend(), Collectors()
    instance = MarketDataService(tmp_path, backend=backend, collectors=collectors)
    yield instance
    backend.block.set()
    instance.close()


def finished(service, identity):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        status = service.status(identity)
        if status["state"] not in {"queued", "running"}:
            return status
        threading.Event().wait(0.005)
    raise AssertionError("fake database action did not finish")


class RulesBackend(Backend):
    def __init__(self, now):
        super().__init__()
        self.now, self.catalog_owners = now, []
        self.catalog_entered = threading.Event()
        self.catalog_release = threading.Event()
        self.catalog_release.set()
        self.fail_catalog = False
        self.reference_time = None

    def catalog(self, market):
        self.catalog_owners.append(threading.get_ident())
        self.catalog_entered.set()
        assert self.catalog_release.wait(3)
        if self.fail_catalog:
            raise StorageError("secret diagnostic must not enter public rules status")
        instrument = Instrument("binance", Market(market), "BTCUSDT")
        rules = ExecutionRulesSnapshot(
            instrument,
            self.now[0] if self.reference_time is None else self.reference_time,
            f"binance:{market}:exchangeInfo",
            "TRADING",
            DecimalRange(Decimal(".001"), Decimal(100), Decimal(".001")),
            None,
            DecimalRange(step=Decimal(".01")),
            reductions_allowed=True,
        )
        return {
            instrument.key: {"status": "TRADING", "metadata": {"execution_rules": rules.document()}}
        }


def wait_rules(service, market="usdm"):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        status = service.execution_rules_status(market)
        if not status["pending"] and not status["in_flight"] and status["state"] != "refreshing":
            return status
        threading.Event().wait(0.005)
    raise AssertionError("bounded fake rules refresh did not finish")


def test_rules_getter_is_memory_only_deduplicates_and_creates_no_durable_actions(tmp_path):
    now, monotonic = [900100], [10.0]
    backend = RulesBackend(now)
    backend.catalog_release.clear()
    service = MarketDataService(
        tmp_path,
        backend=backend,
        collectors=Collectors(),
        clock_ms=lambda: now[0],
        monotonic=lambda: monotonic[0],
    )
    try:
        assert service.execution_rules(KEY) is None
        assert backend.catalog_entered.wait(1)
        for _ in range(100):
            assert service.execution_rules(KEY) is None
        assert len(backend.catalog_owners) == 1 and service.actions == {}
        assert service.execution_rules_status("usdm")["in_flight"]
        backend.catalog_release.set()
        assert wait_rules(service)["state"] == "ready"
        first = service.execution_rules(KEY)
        assert first.observed_at_ms == now[0]
        assert backend.catalog_owners == [service.thread.ident]
        assert service.thread.ident != threading.get_ident()
        assert service.execution_rules(KEY) is first
    finally:
        backend.catalog_release.set()
        service.close()


def test_rules_refresh_failure_preserves_original_stale_timestamp_and_bounded_backoff(tmp_path):
    now, monotonic = [900100], [10.0]
    backend = RulesBackend(now)
    service = MarketDataService(
        tmp_path,
        backend=backend,
        collectors=Collectors(),
        clock_ms=lambda: now[0],
        monotonic=lambda: monotonic[0],
    )
    try:
        service.execution_rules(KEY)
        wait_rules(service)
        original = service.execution_rules(KEY)
        now[0] += 60001
        monotonic[0] += 31
        backend.fail_catalog = True
        assert service.execution_rules(KEY) is original
        failed = wait_rules(service)
        assert failed["reason"] == "StorageError" and failed["retry_after_ms"] == 1000
        assert original.observed_at_ms == 900100
        for _ in range(20):
            assert service.execution_rules(KEY) is original
        assert len(backend.catalog_owners) == 2
        monotonic[0] += 1
        service.execution_rules(KEY)
        assert wait_rules(service)["retry_after_ms"] == 2000
        assert len(backend.catalog_owners) == 3
        assert "secret diagnostic" not in str(service.execution_rules_status("usdm"))
    finally:
        service.close()


def test_explicit_catalog_action_populates_same_cache_and_bad_symbol_is_isolated(tmp_path):
    now = [900100]
    backend = RulesBackend(now)
    original = backend.catalog

    def catalog(market):
        result = original(market)
        broken = deepcopy(result[KEY])
        broken["metadata"]["execution_rules"]["instrument"] = "binance:usdm:ETHUSDT"
        result["binance:usdm:ETHUSDT"] = broken
        return result

    backend.catalog = catalog
    service = MarketDataService(
        tmp_path, backend=backend, collectors=Collectors(), clock_ms=lambda: now[0]
    )
    try:
        service.submit("explicit-rules", "catalog.refresh", {"market": "usdm"})
        assert finished(service, "explicit-rules")["state"] == "complete"
        assert service.execution_rules(KEY) is not None
        assert service.execution_rules("binance:usdm:ETHUSDT") is None
        bad = service.catalogs["usdm"]["binance:usdm:ETHUSDT"]["metadata"]
        assert "execution_rules" not in bad and bad["execution_rules_error"]
    finally:
        service.close()


def test_closed_or_recording_failed_rules_owner_never_requeues_or_returns_fresh_reference(tmp_path):
    now = [900100]
    backend = RulesBackend(now)
    service = MarketDataService(
        tmp_path, backend=backend, collectors=Collectors(), clock_ms=lambda: now[0]
    )
    service.execution_rules(KEY)
    wait_rules(service)
    service.storage_failed = True
    assert service.execution_rules(KEY) is None
    assert not service.execution_rules_status("usdm")["recording_available"]
    service.storage_failed = False
    service.close()
    calls = len(backend.catalog_owners)
    assert service.execution_rules(KEY) is None and service.execution_rules_status("usdm")["closed"]
    assert len(backend.catalog_owners) == calls


def test_backend_catalog_uses_one_real_completion_clock_and_isolates_invalid_symbol(
    tmp_path, monkeypatch
):
    from omega.infrastructure.binance.market import SymbolRules

    key = Instrument("binance", Market.USDM, "BTCUSDT")
    filters = (
        {"filterType": "LOT_SIZE", "minQty": ".001", "maxQty": "100", "stepSize": ".001"},
        {"filterType": "PRICE_FILTER", "minPrice": "0", "maxPrice": "0", "tickSize": ".01"},
    )
    good = SymbolRules(key, "BTC", "USDT", "TRADING", filters)
    bad = SymbolRules(
        Instrument("binance", Market.USDM, "ETHUSDT"),
        "ETH",
        "USDT",
        "TRADING",
        ({"filterType": "LOT_SIZE"},),
    )
    returned, clocks = [], []

    class History:
        def symbols(self):
            returned.append(True)
            return good, bad

    def clock():
        assert returned
        clocks.append(True)
        return 900100000000

    backend = MarketDataBackend(tmp_path)
    monkeypatch.setattr(backend, "history", lambda market: History())
    monkeypatch.setattr("omega.infrastructure.market_data.time.time_ns", clock)
    result = backend.catalog("usdm")
    document = result[KEY]["metadata"]["execution_rules"]
    assert ExecutionRulesSnapshot.from_document(document).observed_at_ms == 900100
    assert result[bad.instrument.key]["metadata"]["execution_rules_error"]
    assert "execution_rules" not in result[bad.instrument.key]["metadata"] and len(clocks) == 1


@pytest.mark.parametrize("offset", [-60001, 60001])
def test_stale_or_future_catalog_document_is_never_retimestamped_as_fresh(tmp_path, offset):
    now, monotonic = [900100], [10.0]
    backend = RulesBackend(now)
    backend.reference_time = now[0] + offset
    service = MarketDataService(
        tmp_path,
        backend=backend,
        collectors=Collectors(),
        clock_ms=lambda: now[0],
        monotonic=lambda: monotonic[0],
    )
    try:
        assert service.execution_rules(KEY) is None
        assert wait_rules(service)["state"] == "unavailable"
        returned = service.execution_rules(KEY)
        assert returned.observed_at_ms == backend.reference_time
        for _ in range(20):
            assert service.execution_rules(KEY) is returned
        assert len(backend.catalog_owners) == 1
    finally:
        service.close()


def test_rules_wait_behind_busy_database_worker_without_blocking_cached_getter(tmp_path):
    now = [900100]
    backend = RulesBackend(now)
    backend.block.clear()
    service = MarketDataService(
        tmp_path, backend=backend, collectors=Collectors(), clock_ms=lambda: now[0]
    )
    try:
        service.snapshot("usdm", KEY)
        assert backend.entered.wait(1)
        started = time.monotonic()
        assert service.execution_rules(KEY) is None
        assert service.execution_rules_status("usdm")["pending"]
        assert time.monotonic() - started < 0.5 and backend.catalog_owners == []
        backend.block.set()
        assert wait_rules(service)["state"] == "ready"
    finally:
        backend.block.set()
        service.close()


def test_snapshot_is_cached_while_database_refresh_is_blocked(service):
    service.backend.block.clear()
    first = service.snapshot("usdm", KEY)
    assert first["source"] == "unavailable"
    assert first["selected"]["channels"]["ohlcv"]["rows"] is None
    assert service.backend.entered.wait(1)
    second = service.snapshot("usdm", KEY)
    assert second["database"]["state"] == "unavailable"
    assert not second["capabilities"]["compression"] and not second["capabilities"]["reset"]
    service.backend.block.set()


def test_catalog_and_selected_metadata_are_real_source_or_unknown(service):
    service.snapshot("usdm", KEY)
    service.submit("catalog", "catalog.refresh", {"market": "usdm"})
    assert finished(service, "catalog")["state"] == "complete"
    snapshot = service.snapshot("usdm", KEY)
    assert snapshot["instruments"][0]["metadata"]["base_asset"] == "BTC"
    assert snapshot["instruments"][0]["dummy_rate"] is None
    assert snapshot["instruments"][0]["gap_rate"] == 1 / 3
    assert not snapshot["selected"]["channels"]["depth"]["supported"]


def test_worker_running_is_distinct_from_requested_collection_and_data_readiness(service):
    service.submit(
        "enable-health",
        "collection.set",
        {"instruments": [KEY], "stream": True, "historical": False},
    )
    assert finished(service, "enable-health")["state"] == "complete"
    service.submit("catalog-health", "catalog.refresh", {"market": "usdm"})
    assert finished(service, "catalog-health")["state"] == "complete"
    row = service.snapshot()["instruments"][0]
    assert row["collection_requested"] and row["collecting_stream"]
    assert not row["historical_requested"]
    assert not row["data_ready"] and row["collection_state"] == "unobserved"
    service.collectors.status = lambda market: {
        "state": "gap",
        "running": True,
        "ready": False,
        "instrument_ready": {KEY: False},
        "last_close_ms": {KEY: 60_000},
    }
    snapshot = service.snapshot()
    assert snapshot["collection"]["worker"]["state"] == "gap"
    assert snapshot["instruments"][0]["data_ready"] is False
    service.collectors.status = lambda market: {
        "state": "ready",
        "running": True,
        "ready": True,
        "instrument_ready": {KEY: True},
        "last_close_ms": {KEY: 180_000},
    }
    assert service.snapshot()["instruments"][0]["data_ready"] is True


def test_exchange_catalog_can_still_be_shown_when_local_database_is_unavailable(service):
    service.backend.fail_refresh = True
    service.submit("catalog", "catalog.refresh", {"market": "usdm"})
    assert finished(service, "catalog")["state"] == "complete"
    snapshot = service.snapshot()
    assert snapshot["source"] == "unavailable"
    assert snapshot["instruments"][0]["metadata"]["quote_asset"] == "USDT"
    assert snapshot["instruments"][0]["rows"] is None


def test_duplicate_action_never_repeats_and_changed_public_parameters_conflict(service):
    parameters = {"instrument": KEY, "lookback_minutes": 3}
    service.submit("repair", "gaps.refetch", parameters)
    assert finished(service, "repair")["result"]["inserted_rows"] == 1
    assert service.submit("repair", "gaps.refetch", parameters)["state"] == "complete"
    assert len([call for call in service.backend.calls if call[0] == "refetch"]) == 1
    with pytest.raises(ValidationError):
        service.submit("repair", "gaps.refetch", {**parameters, "lookback_minutes": 4})


def test_peer_password_connection_and_exception_text_never_enter_record_or_view(service):
    parameters = {
        "instrument": KEY,
        "host": "192.168.1.2",
        "port": 55432,
        "database": "omega",
        "user": "remote_user",
        "password": "fail-secret",
        "lookback_minutes": 3,
    }
    original = deepcopy(parameters)
    service.submit("peer", "peer.import", parameters)
    status = finished(service, "peer")
    assert status["state"] == "failed" and status["error"] == "RuntimeError"
    assert parameters == original
    payload = service.path.read_text() + json.dumps(status) + json.dumps(service.snapshot())
    for private in ("fail-secret", "remote_user", "192.168.1.2", '"password"'):
        assert private not in payload


def test_collection_changes_only_selected_targets_and_preserves_other_history_plan(service):
    other = "binance:usdm:ETHUSDT"
    service.submit(
        "enable",
        "collection.set",
        {"instruments": [KEY, other], "stream": True, "historical": True, "lookback_minutes": 2000},
    )
    result = finished(service, "enable")
    assert result["result"]["confirmation"] == "worker_configured_not_data_ready"
    service.submit(
        "stop-one", "collection.set", {"instruments": [KEY], "stream": False, "historical": False}
    )
    assert finished(service, "stop-one")["state"] == "complete"
    assert service.collectors.calls[-1] == ("usdm", [other], 2000, True)
    assert service.collectors.plans[-1] == {other: 2000}


def test_stream_only_and_historical_symbols_keep_distinct_history_windows(service):
    other = "binance:usdm:ETHUSDT"
    service.submit(
        "stream", "collection.set", {"instruments": [KEY], "stream": True, "historical": False}
    )
    assert finished(service, "stream")["state"] == "complete"
    service.submit(
        "history",
        "collection.set",
        {"instruments": [other], "stream": True, "historical": True, "lookback_minutes": 20000},
    )
    assert finished(service, "history")["state"] == "complete"
    assert service.collectors.plans[-1] == {KEY: 0, other: 20000}
    service.submit("catalog", "catalog.refresh", {"market": "usdm"})
    assert finished(service, "catalog")["state"] == "complete"
    snapshot = service.snapshot()
    modes = {row["instrument"]: row["collecting_historical"] for row in snapshot["instruments"]}
    assert modes[KEY] is False and modes[other] is True


def test_restart_preserves_collection_wishes_without_starting_or_replaying_any_worker(tmp_path):
    first = MarketDataService(tmp_path, backend=Backend(), collectors=Collectors())
    try:
        first.submit(
            "persist-plan",
            "collection.set",
            {
                "instruments": [KEY],
                "stream": True,
                "historical": True,
                "lookback_minutes": 2000,
            },
        )
        assert finished(first, "persist-plan")["state"] == "complete"
        first.submit("plan-catalog", "catalog.refresh", {"market": "usdm"})
        assert finished(first, "plan-catalog")["state"] == "complete"
    finally:
        first.close()
    saved = json.loads(first.path.read_text())
    assert saved["schema_version"] == 2
    saved["actions"]["persist-plan"]["status"]["state"] = "running"
    first.path.write_text(json.dumps(saved))
    collectors, backend = Collectors(), Backend()
    backend.fail_refresh = True
    second = MarketDataService(tmp_path, backend=backend, collectors=collectors)
    try:
        assert second.status("persist-plan")["state"] == "unknown"
        assert not collectors.calls
        view = second.snapshot()
        row = next(row for row in view["instruments"] if row["instrument"] == KEY)
        assert row["collection_requested"] and row["historical_requested"]
        assert row["history_lookback_minutes"] == 2000
        assert not row["collecting_stream"] and not row["data_ready"]
        assert view["collection"]["manual_apply_required"]
        assert not collectors.calls
        second.submit(
            "manual-reapply",
            "collection.set",
            {
                "instruments": [KEY],
                "stream": True,
                "historical": True,
                "lookback_minutes": 2000,
            },
        )
        assert finished(second, "manual-reapply")["state"] == "complete"
        assert collectors.calls == [("usdm", [KEY], 2000, True)]
    finally:
        second.close()


def test_version_one_operation_journal_upgrades_without_inventing_collection_wishes(tmp_path):
    path = tmp_path / "state" / "market-data.json"
    path.parent.mkdir()
    path.write_text(json.dumps({"schema_version": 1, "actions": {}}))
    collectors = Collectors()
    instance = MarketDataService(tmp_path, backend=Backend(), collectors=collectors)
    try:
        assert instance.desired == {}
        assert not collectors.calls
        upgraded = json.loads(path.read_text())
        assert upgraded == {"schema_version": 2, "actions": {}, "desired": {}}
    finally:
        instance.close()


@pytest.mark.parametrize(
    "plan",
    [
        [],
        {KEY: True},
        {KEY: {"stream": True, "historical": True, "lookback_minutes": True}},
        {KEY: {"stream": False, "historical": True, "lookback_minutes": 500}},
    ],
)
def test_invalid_saved_collection_wishes_fail_closed_without_starting_children(tmp_path, plan):
    path = tmp_path / "state" / "market-data.json"
    path.parent.mkdir()
    content = json.dumps({"schema_version": 2, "actions": {}, "desired": plan})
    path.write_text(content)
    collectors = Collectors()
    with pytest.raises(StorageError):
        MarketDataService(tmp_path, backend=Backend(), collectors=collectors)
    assert not collectors.calls and path.read_text() == content


def test_disabling_collection_does_not_restore_it_on_the_next_start(tmp_path):
    first = MarketDataService(tmp_path, backend=Backend(), collectors=Collectors())
    try:
        for identity, enabled in (("on", True), ("off", False)):
            first.submit(
                identity,
                "collection.set",
                {
                    "instruments": [KEY],
                    "stream": enabled,
                    "historical": enabled,
                },
            )
            assert finished(first, identity)["state"] == "complete"
    finally:
        first.close()
    second = MarketDataService(tmp_path, backend=Backend(), collectors=Collectors())
    try:
        assert not second.desired
        assert not second.collectors.calls
    finally:
        second.close()


@pytest.mark.parametrize(
    "kind,parameters",
    [
        ("collection.set", {"instruments": [KEY], "stream": False, "historical": True}),
        ("collection.set", {"instruments": [], "stream": True, "historical": False}),
        ("gaps.refetch", {"instrument": KEY, "lookback_minutes": 20001}),
        ("peer.import", {"instrument": KEY, "host": "8.8.8.8"}),
    ],
)
def test_invalid_or_unsupported_requests_fail_before_enqueuing(service, kind, parameters):
    with pytest.raises(ValidationError):
        service.submit("invalid", kind, parameters)
    assert not service.actions


def test_restart_marks_unconfirmed_action_unknown_and_never_reexecutes(tmp_path):
    backend, collectors = Backend(), Collectors()
    first = MarketDataService(tmp_path, backend=backend, collectors=collectors)
    first.submit("done", "gaps.refetch", {"instrument": KEY})
    finished(first, "done")
    first.close()
    saved = json.loads(first.path.read_text())
    saved["actions"]["done"]["status"]["state"] = "running"
    first.path.write_text(json.dumps(saved))
    second = MarketDataService(tmp_path, backend=backend, collectors=Collectors())
    try:
        assert second.status("done")["state"] == "unknown"
        assert second.submit("done", "gaps.refetch", {"instrument": KEY})["state"] == "unknown"
        assert len([call for call in backend.calls if call[0] == "refetch"]) == 1
    finally:
        second.close()


def test_bounded_pending_queue_and_one_worker(service):
    service.backend.block.clear()
    service.submit("active", "catalog.refresh", {})
    deadline = time.monotonic() + 1
    while service.status("active")["state"] != "running" and time.monotonic() < deadline:
        threading.Event().wait(0.005)
    for index in range(8):
        service.submit(f"queued{index}", "catalog.refresh", {})
    with pytest.raises(ValidationError, match="capacity"):
        service.submit("overflow", "catalog.refresh", {})
    assert len(service.queue) == 8
    service.backend.block.set()


def test_recording_failure_before_dispatch_does_not_execute_an_operation(service, monkeypatch):
    import omega.runtime.market_data_service as module

    with monkeypatch.context() as patch:
        patch.setattr(module, "atomic_json", lambda *_: (_ for _ in ()).throw(OSError("full")))
        with pytest.raises(StorageError):
            service.submit("blocked", "gaps.refetch", {"instrument": KEY})
        assert service.status("blocked")["state"] == "unknown"
        assert not [call for call in service.backend.calls if call[0] == "refetch"]


def test_final_recording_failure_is_unknown_and_never_repeats_a_completed_effect(
    service, monkeypatch
):
    import omega.runtime.market_data_service as module

    original, count = module.atomic_json, [0]

    def failing(path, value):
        count[0] += 1
        if count[0] >= 3:
            raise OSError("full after data commit")
        original(path, value)

    with monkeypatch.context() as patch:
        patch.setattr(module, "atomic_json", failing)
        service.submit("uncertain", "gaps.refetch", {"instrument": KEY})
        assert finished(service, "uncertain")["state"] == "unknown"
        assert (
            service.submit("uncertain", "gaps.refetch", {"instrument": KEY})["state"] == "unknown"
        )
        assert len([call for call in service.backend.calls if call[0] == "refetch"]) == 1
        with pytest.raises(StorageError):
            service.submit("new", "gaps.refetch", {"instrument": KEY})


def test_failed_child_shutdown_still_joins_operation_worker_and_reports_failure(tmp_path):
    class UnconfirmedCollectors(Collectors):
        def close(self):
            raise StorageError("child did not stop")

    instance = MarketDataService(tmp_path, backend=Backend(), collectors=UnconfirmedCollectors())
    with pytest.raises(StorageError, match="shutdown was not confirmed"):
        instance.close()
    assert instance.closed and not instance.thread.is_alive()
    assert instance.path.exists()


def test_close_clears_queued_peer_credentials_without_executing_them(tmp_path):
    backend = Backend()
    backend.block.clear()
    instance = MarketDataService(tmp_path, backend=backend, collectors=Collectors())
    instance.submit("active", "catalog.refresh", {})
    deadline = time.monotonic() + 1
    while instance.status("active")["state"] != "running" and time.monotonic() < deadline:
        threading.Event().wait(0.005)
    parameters = {
        "instrument": KEY,
        "host": "192.168.1.2",
        "database": "omega",
        "user": "remote_user",
        "password": "queued-secret",
    }
    instance.submit("queued-peer", "peer.import", parameters)
    queued = instance.queue[0][1]
    shutdown = threading.Thread(target=instance.close)
    shutdown.start()
    assert instance.stop.wait(1)
    backend.block.set()
    shutdown.join(3)
    assert not shutdown.is_alive()
    assert not queued and not instance.thread.is_alive()
    assert not [call for call in backend.calls if call[0] == "peer"]
    assert instance.status("queued-peer")["state"] == "failed"


@pytest.mark.parametrize(
    "operation", ["catalog.refresh", "gaps.refetch", "refresh", "refresh.error"]
)
def test_retired_worker_cannot_replace_next_owner_journal_or_update_cached_state(
    tmp_path, monkeypatch, operation
):
    entered, release = threading.Event(), threading.Event()

    class BlockingBackend(Backend):
        def wait(self):
            entered.set()
            assert release.wait(5)

        def catalog(self, market):
            if operation == "catalog.refresh":
                self.wait()
            return super().catalog(market)

        def refetch(self, key, lookback, progress, stop):
            self.wait()
            progress(0.99)  # Callback arrives after close's bounded join expired.
            return {"inserted_rows": 1, "remaining_ranges": 0, "observed_only": True}

        def refresh(self, market, selected):
            if operation.startswith("refresh"):
                self.wait()
                if operation == "refresh.error":
                    raise StorageError("late database failure")
            return super().refresh(market, selected)

    old = MarketDataService(tmp_path, backend=BlockingBackend(), collectors=Collectors())
    original_join = old.thread.join
    new = None
    try:
        if operation.startswith("refresh"):
            old.snapshot(instrument=KEY)
        else:
            parameters = {"instrument": KEY} if operation == "gaps.refetch" else {}
            old.submit("old-operation", operation, parameters)
        assert entered.wait(2)
        # Model the real 30s deadline without sleeping or running any real I/O.
        with monkeypatch.context() as patch:
            patch.setattr(old.thread, "join", lambda timeout=None: None)
            with pytest.raises(StorageError, match="still stopping"):
                old.close()
        frozen_views, frozen_catalogs = deepcopy(old.views), deepcopy(old.catalogs)
        frozen_actions, frozen_refresh_at = deepcopy(old.actions), dict(old.refresh_at)
        new = MarketDataService(tmp_path, backend=Backend(), collectors=Collectors())
        persisted = threading.Event()
        save = new._save

        def saved(*args, **kwargs):
            save(*args, **kwargs)
            if new.actions.get("new-owner", {}).get("status", {}).get("state") == "complete":
                persisted.set()

        monkeypatch.setattr(new, "_save", saved)
        new.submit("new-owner", "catalog.refresh", {})
        assert finished(new, "new-owner")["state"] == "complete"
        assert persisted.wait(2)
        expected = new.path.read_bytes()
        release.set()
        original_join(2)
        assert not old.thread.is_alive()
        assert old.path.read_bytes() == expected
        assert old.views == frozen_views and old.catalogs == frozen_catalogs
        assert old.actions == frozen_actions and old.refresh_at == frozen_refresh_at
        assert not old.active_historical
        # A second old-owner close/save must not replace the newer journal either.
        old.close()
        old._save()
        assert old.path.read_bytes() == expected
    finally:
        release.set()
        original_join(2)
        old.close()
        if new is not None:
            new.close()


def test_repeated_close_does_not_hide_unconfirmed_collectors(tmp_path):
    class Unconfirmed(Collectors):
        def close(self):
            raise StorageError("still alive")

    instance = MarketDataService(tmp_path, backend=Backend(), collectors=Unconfirmed())
    for _ in range(2):
        with pytest.raises(StorageError, match="shutdown was not confirmed"):
            instance.close()


def test_retired_operation_or_refresh_does_not_start_a_backend_call(tmp_path):
    backend = Backend()
    instance = MarketDataService(tmp_path, backend=backend, collectors=Collectors())
    instance.close()
    with pytest.raises(InterruptedError, match="stopping"):
        instance._perform("catalog.refresh", {"market": "usdm"}, lambda _: None)
    instance._refresh("usdm", None)
    assert not backend.calls
