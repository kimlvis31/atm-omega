"""Public resource wiring uses real temp configuration/budgets and fake HTTP/DB ports."""

import threading
from dataclasses import replace
from urllib.parse import parse_qs, urlsplit

import pytest

from omega.domain.errors import RateLimited, StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.auxiliary_budget import AuxiliaryPublicBudget
from omega.infrastructure.binance.auxiliary_client import BinanceAuxiliaryClient
from omega.infrastructure.binance.transport import HttpResponse
from omega.infrastructure.postgres.config import DatabaseSettings, database_destination_id
from omega.infrastructure.raw_archive import FileRawArchive
from omega.runtime import auxiliary_resources as module
from omega.runtime.auxiliary_resources import PublicAuxiliaryResources, PublicDepthResources

NOW = 1_793_000_100_000


def configuration(root, **changes):
    settings = replace(
        DatabaseSettings("127.0.0.1", 55433, "omega_test", "omega_app", "fake-test-secret"),
        **changes,
    )
    root.mkdir(parents=True, exist_ok=True)
    (root / "password.txt").write_text(settings.password)
    (root / "database.config.toml").write_text(
        f'host="{settings.host}"\nport={settings.port}\ndbname="{settings.dbname}"\nuser="{settings.user}"\npassword_file="password.txt"\n'
    )
    return settings


class Transport:
    def __init__(self):
        self.requests = []

    def send(self, request):
        self.requests.append(request)
        return HttpResponse(200, b"[]", {})


class Store:
    def __init__(self, settings):
        self.destination_id = database_destination_id(settings)
        self.owner, self.closed = threading.get_ident(), False

    def close(self):
        assert threading.get_ident() == self.owner
        self.closed = True


def plan(kind="trade_flow", instrument="binance:usdm:ETHUSDT", **extra):
    return {"kind": kind, "parameters": {"instrument": instrument}, **extra}


def test_constructor_is_lazy_even_when_configuration_does_not_exist(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("constructor must not load settings or create resources")

    monkeypatch.setattr(module, "load_database_settings", forbidden)
    factory = PublicAuxiliaryResources(
        tmp_path / "missing", "a" * 64, transport_factory=forbidden, store_factory=forbidden
    )
    assert isinstance(factory, PublicAuxiliaryResources)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_depth_factory_preserves_local_binding_and_public_only_shared_budget(tmp_path, market):
    settings, transport = configuration(tmp_path), Transport()
    factory = PublicDepthResources(
        tmp_path,
        database_destination_id(settings),
        lambda: NOW,
        transport_factory=lambda: transport,
        store_factory=Store,
    )
    client, archive, store = factory(Instrument("binance", market, "ETHUSDT"))
    try:
        assert client.credentials is None and client.enable_trading is False
        assert client.ip_scope == "binance-ip" and isinstance(client.budget, AuxiliaryPublicBudget)
        assert isinstance(
            archive, FileRawArchive
        ) and store.destination_id == database_destination_id(settings)
        assert transport.requests == []
    finally:
        store.close()


def test_depth_factory_wrong_destination_fails_before_any_http_or_store(tmp_path):
    configuration(tmp_path)

    def forbidden(*args):
        pytest.fail("invalid binding must not allocate HTTP or DB")

    factory = PublicDepthResources(
        tmp_path, "a" * 64, transport_factory=forbidden, store_factory=forbidden
    )
    with pytest.raises(ValidationError, match="bound local"):
        factory(Instrument("binance", Market.USDM, "ETHUSDT"))


@pytest.mark.parametrize(
    "market,origin", [(Market.SPOT, "api.binance.com"), (Market.USDM, "fapi.binance.com")]
)
def test_exact_public_client_archive_and_shared_budgets_are_composed(tmp_path, market, origin):
    settings, transport = configuration(tmp_path), Transport()
    factory = PublicAuxiliaryResources(
        tmp_path,
        database_destination_id(settings),
        lambda: NOW,
        transport_factory=lambda: transport,
        store_factory=Store,
    )
    client, archive, store = factory(plan(instrument=f"binance:{market.value}:ETHUSDT"))
    try:
        assert isinstance(client, BinanceAuxiliaryClient) and isinstance(archive, FileRawArchive)
        assert client.market == market and client.client.credentials is None
        assert client.client.enable_trading is False and client.client.ip_scope == "binance-ip"
        budget = client.client.budget
        assert isinstance(budget, AuxiliaryPublicBudget)
        assert budget.primary.path == tmp_path / "state/binance-public-budget.json"
        assert budget.requests.path == tmp_path / "state/binance-public-aux-request-budget.json"
        assert transport.requests == []
        client.agg_trades(
            Instrument("binance", market, "ETHUSDT"), start_ms=NOW - 60000, end_ms=NOW, limit=1
        )
        request = transport.requests[0]
        assert request.method == "GET" and urlsplit(request.url).hostname == origin
        assert not request.headers and request.body is None
        query = parse_qs(urlsplit(request.url).query)
        assert query["symbol"] == ["ETHUSDT"] and "signature" not in query
        assert "fake-test-secret" not in request.url
        assert archive.root == tmp_path / "state/auxiliary-raw"
    finally:
        store.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"host": "example.com"},
        {"host": "localhost"},
        {"port": 5432},
        {"dbname": "legacy_eta"},
        {"user": "postgres"},
    ],
)
def test_other_destinations_fail_before_transport_archive_or_store(tmp_path, changes):
    settings, called = configuration(tmp_path, **changes), []
    factory = PublicAuxiliaryResources(
        tmp_path,
        database_destination_id(settings),
        transport_factory=lambda: called.append("transport"),
        store_factory=lambda value: called.append("store"),
    )
    with pytest.raises(ValidationError, match="bound local"):
        factory(plan())
    assert called == [] and not (tmp_path / "state").exists()


def test_changed_destination_is_rechecked_on_each_call_before_resources(tmp_path):
    settings, calls = configuration(tmp_path), []
    factory = PublicAuxiliaryResources(
        tmp_path,
        database_destination_id(settings),
        transport_factory=lambda: calls.append("transport"),
        store_factory=Store,
    )
    configuration(tmp_path, port=55432, dbname="omega")
    with pytest.raises(ValidationError, match="bound local"):
        factory(plan())
    assert calls == []


@pytest.mark.parametrize(
    "job_plan",
    [
        plan("current_oi", "binance:spot:ETHUSDT"),
        plan("history_oi", "binance:spot:ETHUSDT"),
        plan("long_short_ratio", "binance:spot:ETHUSDT"),
        plan("orders"),
        plan(instrument="binance:usdm:ethusdt"),
        plan(instrument="other:usdm:ETHUSDT"),
        plan(channel="depth"),
        {},
    ],
)
def test_invalid_market_kind_or_channel_is_rejected_before_loading_configuration(
    tmp_path, job_plan, monkeypatch
):
    monkeypatch.setattr(
        module,
        "load_database_settings",
        lambda path: pytest.fail("invalid public plan must fail before config IO"),
    )
    factory = PublicAuxiliaryResources(tmp_path, "a" * 64, store_factory=Store)
    with pytest.raises(ValidationError):
        factory(job_plan)


@pytest.mark.parametrize(
    "kind", ["current_oi", "history_oi", "long_short_ratio", "trade_flow_repair"]
)
def test_supported_job_kinds_bind_canonical_market_without_network(tmp_path, kind):
    settings, transport = configuration(tmp_path), Transport()
    factory = PublicAuxiliaryResources(
        tmp_path,
        database_destination_id(settings),
        transport_factory=lambda: transport,
        store_factory=Store,
    )
    client, _, store = factory(plan(kind, id="existing-service-row", state="running"))
    try:
        assert client.market == Market.USDM and transport.requests == []
    finally:
        store.close()


def test_store_destination_mismatch_is_closed_on_its_creating_worker(tmp_path):
    settings, stores = configuration(tmp_path), []

    def wrong(value):
        store = Store(value)
        store.destination_id = "b" * 64
        stores.append(store)
        return store

    factory = PublicAuxiliaryResources(
        tmp_path,
        database_destination_id(settings),
        transport_factory=Transport,
        store_factory=wrong,
    )
    with pytest.raises(ValidationError, match="store destination"):
        factory(plan())
    assert stores[0].closed


def test_mismatched_store_cleanup_error_is_sanitized(tmp_path):
    settings = configuration(tmp_path)

    class BadStore(Store):
        destination_id = "b" * 64

        def __init__(self, settings):
            self.destination_id = "b" * 64

        def close(self):
            raise RuntimeError("private DSN fake-test-secret")

    factory = PublicAuxiliaryResources(
        tmp_path,
        database_destination_id(settings),
        transport_factory=Transport,
        store_factory=BadStore,
    )
    with pytest.raises(StorageError, match="cleanup failed") as error:
        factory(plan())
    assert "fake-test-secret" not in str(error.value)


def test_spot_and_usdm_factories_share_actual_primary_admission_before_fake_http(tmp_path):
    settings, spot_transport, futures_transport = configuration(tmp_path), Transport(), Transport()
    destination = database_destination_id(settings)
    spot, _, spot_store = PublicAuxiliaryResources(
        tmp_path,
        destination,
        lambda: NOW,
        transport_factory=lambda: spot_transport,
        store_factory=Store,
    )(plan(instrument="binance:spot:ETHUSDT"))
    futures, _, futures_store = PublicAuxiliaryResources(
        tmp_path,
        destination,
        lambda: NOW,
        transport_factory=lambda: futures_transport,
        store_factory=Store,
    )(plan())
    try:
        spot.client.budget.primary.reserve({"binance-ip": 590})
        with pytest.raises(RateLimited):
            futures.agg_trades(
                Instrument("binance", Market.USDM, "ETHUSDT"),
                start_ms=NOW - 60000,
                end_ms=NOW,
                limit=1,
            )
        assert futures_transport.requests == []
        spot.agg_trades(
            Instrument("binance", Market.SPOT, "ETHUSDT"), start_ms=NOW - 60000, end_ms=NOW, limit=1
        )
        assert len(spot_transport.requests) == 1
    finally:
        spot_store.close()
        futures_store.close()


def test_production_local_identity_and_resource_creation_use_caller_worker(tmp_path):
    settings = configuration(tmp_path, port=55432, dbname="omega")
    created, errors = [], []
    factory = PublicAuxiliaryResources(
        tmp_path,
        database_destination_id(settings),
        transport_factory=Transport,
        store_factory=Store,
    )

    def run():
        try:
            client, archive, store = factory(plan())
            created.append((threading.get_ident(), store.owner, client.market, archive.root))
            store.close()
        except Exception as exc:
            errors.append(type(exc).__name__)

    worker = threading.Thread(target=run)
    worker.start()
    worker.join(2)
    assert not worker.is_alive() and errors == []
    assert created[0][0] == created[0][1] != threading.get_ident()
    assert created[0][2:] == (Market.USDM, tmp_path / "state/auxiliary-raw")
