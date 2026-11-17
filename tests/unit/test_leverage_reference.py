"""All signed traffic uses mock transport and fake credentials, never an account/API."""

import json
import threading
from copy import deepcopy
from decimal import Decimal
from urllib.parse import urlsplit

import pytest

from omega.domain.errors import ProtocolError, TradingBlocked, ValidationError
from omega.infrastructure.binance.client import BinanceClient, Credentials
from omega.infrastructure.binance.leverage import BinanceLeverageBrackets, normalize_brackets
from omega.infrastructure.binance.transport import HttpResponse
from omega.runtime import leverage_reference as module

RAW = """[{"symbol":"BTCUSDT","notionalCoef":1.5,"brackets":[
{"bracket":1,"initialLeverage":100,"notionalFloor":0,"notionalCap":100.125,"maintMarginRatio":0.004,"cum":0},
{"bracket":2,"initialLeverage":50,"notionalFloor":100.125,"notionalCap":200.875,"maintMarginRatio":0.005,"cum":0.1}]}]"""


class Transport:
    def __init__(self):
        self.calls = []

    def send(self, request):
        self.calls.append(request)
        return HttpResponse(200, RAW.encode())


def raw():
    return json.loads(RAW, parse_float=Decimal)


def envelope(account_id="account_a"):
    return {
        "schema_version": 1,
        "provider": "binance",
        "market": "usdm",
        "account_id": account_id,
        "source": "binance_signed_account",
        "table": normalize_brackets(raw()),
    }


class Provider:
    def __init__(self, value=None):
        self.value = envelope() if value is None else value
        self.calls, self.release, self.entered = 0, threading.Event(), threading.Event()
        self.release.set()
        self.failure = False

    def fetch(self):
        self.calls += 1
        self.entered.set()
        assert self.release.wait(2)
        if self.failure:
            raise RuntimeError("PRIVATE_KEY_AND_SIGNED_URL_MUST_NOT_LEAK")
        return deepcopy(self.value)


@pytest.fixture
def services(tmp_path):
    instances = []

    def create(provider=None, account_id="account_a", now=10000):
        instance = module.LeverageReferenceService(
            tmp_path,
            provider=provider,
            account_id=account_id,
            clock_ms=lambda: now,
            max_age_ms=1000,
        )
        instances.append(instance)
        return instance

    yield create
    for instance in instances:
        if instance.provider:
            instance.provider.release.set()
        instance.close()


def complete(instance):
    if instance.thread:
        instance.thread.join(2)
        assert not instance.thread.is_alive()
    return instance.status()


def test_unicode_symbol_reference_roundtrip_keeps_native_identity(services):
    payload = raw()
    payload[0]["symbol"] = "币安人生USDT"
    reference = {**envelope(), "table": normalize_brackets(payload)}
    instance = services(Provider(reference))
    instance.start()
    assert complete(instance)["state"] == "ready"
    assert set(instance.table("币安人生USDT")["reference"]["table"]) == {"币安人生USDT"}
    assert set(
        module.LeverageReferenceService(
            instance.path.parents[2], account_id="account_a", clock_ms=lambda: 10000
        ).table()["reference"]["table"]
    ) == {"币安人生USDT"}
    for unsafe in ("ＢＴＣUSDT", "BTC/USDT", "币安\u202eUSDT", "btcUSDT"):
        with pytest.raises(ValidationError):
            instance.table(unsafe)
        payload[0]["symbol"] = unsafe
        with pytest.raises(ProtocolError):
            normalize_brackets(payload)


def test_adapter_is_exclusively_signed_readonly_get_with_exact_decimal_bounds():
    transport = Transport()
    client = BinanceClient(
        "https://fapi.binance.com",
        transport,
        credentials=Credentials("fake-api-key", "fake-secret"),
        clock_ms=lambda: 123456,
    )
    value = BinanceLeverageBrackets(client, "account_a").fetch()
    assert len(transport.calls) == 1
    request = transport.calls[0]
    assert request.method == "GET" and request.body is None
    assert urlsplit(request.url).path == "/fapi/v1/leverageBracket"
    assert "signature=" in request.url and "timestamp=123456" in request.url
    assert request.headers["X-MBX-APIKEY"] == "fake-api-key"
    symbol = value["table"]["BTCUSDT"]
    assert symbol["notional_coefficient"] == "1.5" and symbol["coefficient_applied"] is False
    assert symbol["tiers"][0]["notional_cap"] == "100.125"
    assert symbol["tiers"][1]["notional_floor"] == "100.125"
    assert symbol["tiers"][1]["notional_cap"] == "200.875"


def test_no_credentials_prevents_even_mock_transport_request():
    transport = Transport()
    client = BinanceClient("https://fapi.binance.com", transport)
    with pytest.raises(TradingBlocked):
        BinanceLeverageBrackets(client, "account_a").fetch()
    assert transport.calls == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("notionalCap", "100.124"),
        ("notionalFloor", "100.124"),
        ("notionalCap", "NaN"),
        ("maintMarginRatio", "1"),
        ("cum", "-0.1"),
        ("initialLeverage", True),
        ("bracket", 1),
    ],
)
def test_invalid_or_noncontiguous_tiers_are_rejected(field, value):
    value_raw = raw()
    value_raw[0]["brackets"][1][field] = value
    with pytest.raises(ProtocolError):
        normalize_brackets(value_raw)


@pytest.mark.parametrize("coefficient", ["0", "-1", "NaN", "Infinity", 1.5, True])
def test_invalid_coefficient_cannot_be_silently_scaled_or_cached(coefficient):
    value = raw()
    value[0]["notionalCoef"] = coefficient
    with pytest.raises(ProtocolError):
        normalize_brackets(value)


def test_symbol_response_form_and_sorting_without_duplicate_or_fraction_truncation():
    item = raw()[0]
    item["brackets"].reverse()
    item.pop("notionalCoef")
    table = normalize_brackets(item)
    assert table["BTCUSDT"]["notional_coefficient"] is None
    assert table["BTCUSDT"]["tiers"][0]["bracket"] == 1
    with pytest.raises(ProtocolError):
        normalize_brackets([item, item])


def test_unauthorized_start_is_auth_required_without_network_or_fake_timestamp(services):
    instance = services()
    instance.start()
    assert instance.thread is None and not instance.path.exists()
    assert instance.status()["state"] == "auth_required"
    assert instance.status()["fetched_at_ms"] is None
    assert instance.table()["reference"] is None


def test_start_is_nonblocking_and_valid_cache_reloads_with_age_and_auth_state(services):
    provider = Provider()
    provider.release.clear()
    instance = services(provider)
    instance.start()
    assert provider.entered.wait(1) and instance.status()["state"] == "refreshing"
    provider.release.set()
    assert complete(instance)["state"] == "ready"
    assert provider.calls == 1
    reloaded = services(now=12001)
    reloaded.start()
    status = reloaded.status()
    assert status["state"] == "auth_required" and status["cached"]
    assert status["freshness"] == "stale" and status["age_ms"] == 2001
    assert reloaded.table("BTCUSDT")["reference"]["account_id"] == "account_a"
    assert reloaded.table("ETHUSDT")["reference"]["table"] == {}


@pytest.mark.parametrize("failure", ["scope", "source", "coefficient", "transport"])
def test_failed_refresh_retains_prior_valid_cache_and_redacts_private_error(services, failure):
    first = services(Provider())
    first.start()
    assert complete(first)["state"] == "ready"
    saved = first.path.read_bytes()
    provider = Provider()
    if failure == "scope":
        provider.value["account_id"] = "different_account"
    elif failure == "source":
        provider.value["source"] = "legacy_reference_only"
    elif failure == "coefficient":
        provider.value["table"]["BTCUSDT"]["coefficient_applied"] = True
    else:
        provider.failure = True
    second = services(provider, now=10100)
    second.start()
    assert complete(second)["state"] == "failed"
    assert second.path.read_bytes() == saved
    assert second.table()["reference"] == first.table()["reference"]
    assert "PRIVATE_KEY" not in json.dumps(second.table())


def test_failed_atomic_write_never_publishes_the_candidate_or_destroys_prior_cache(
    services, monkeypatch
):
    first = services(Provider())
    first.start()
    complete(first)
    before = first.path.read_bytes()
    second = services(Provider(), now=11000)
    with monkeypatch.context() as patch:
        patch.setattr(
            module, "atomic_json", lambda *_: (_ for _ in ()).throw(OSError("private path"))
        )
        second.start()
        assert complete(second)["state"] == "failed"
    assert second.path.read_bytes() == before
    assert second.status()["fetched_at_ms"] == 10000


def test_stop_during_fetch_never_commits_a_late_candidate(services):
    provider = Provider()
    provider.release.clear()
    instance = services(provider)
    instance.start()
    assert provider.entered.wait(1)
    close = threading.Thread(target=instance.close)
    close.start()
    assert instance.stopped.wait(1)
    provider.release.set()
    close.join(2)
    assert not close.is_alive() and instance.status()["state"] == "closed"
    assert not instance.path.exists()


def test_scope_mismatched_cache_and_legacy_table_are_not_automatically_used(services):
    instance = services()
    instance.path.parent.mkdir(parents=True)
    instance.path.write_text(json.dumps({"fetched_at_ms": 10000, "reference": envelope("wrong")}))
    reloaded = services()
    reloaded.start()
    assert reloaded.status()["error"] == "CACHE_INVALID"
    assert reloaded.table()["reference"] is None
    instance.path.write_text(json.dumps({"generation_time_utc": "old", "table": {"BTCUSDT": []}}))
    legacy = services()
    legacy.start()
    assert legacy.status()["error"] == "CACHE_INVALID" and not legacy.status()["cached"]


def test_distinct_public_account_namespaces_never_share_cached_brackets(services):
    first = services(Provider())
    first.start()
    assert complete(first)["state"] == "ready"
    other = services(account_id="account_b")
    other.start()
    assert other.path != first.path and not other.status()["cached"]
    assert other.table()["reference"] is None


def test_clock_rollback_is_visible_and_cannot_overwrite_newer_cache(services):
    first = services(Provider())
    first.start()
    complete(first)
    before = first.path.read_bytes()
    second = services(Provider(), now=9000)
    second.start()
    assert complete(second)["state"] == "failed"
    assert second.status()["freshness"] == "clock_skew"
    assert second.path.read_bytes() == before


def test_transport_weight_is_exactly_one_and_not_an_order_reservation():
    class Budget:
        def __init__(self):
            self.costs = []

        def reserve(self, costs):
            self.costs.append(costs)

    budget, transport = Budget(), Transport()
    client = BinanceClient(
        "https://fapi.binance.com",
        transport,
        credentials=Credentials("fake", "fake"),
        budget=budget,
    )
    BinanceLeverageBrackets(client, "account_a").fetch()
    assert budget.costs == [{"binance-ip": 1}]
