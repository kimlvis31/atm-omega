"""Resident feature grace never becomes a financial-owner blocking wait."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from omega.domain.external_features import ExternalFeatureBinding
from omega.domain.models import Candle, Instrument, Market
from omega.runtime import margin_trading
from omega.runtime.margin_trading import MarginTradingService

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def inputs():
    binding = ExternalFeatureBinding(
        "causal_nn", "model", "main", BTC, "a" * 64, "b" * 64, ("NN.score",)
    )
    rows = [{"external_features": [binding.document()]}] * 2
    candle = Candle(BTC, 0, 60000, *(Decimal(value) for value in (100, 101, 99, 100, 10)))
    return {"strategies": rows, "external_feature_wait_ms": 1000}, candle, binding


class Port:
    def __init__(self):
        self.calls, self.disposition, self.failure = [], "pending", False

    def offer_committed(self, binding, candle):
        self.calls.append((binding, candle))
        if self.failure:
            raise RuntimeError("inference is unavailable")
        return SimpleNamespace(disposition=self.disposition)


def test_grace_is_bounded_per_account_and_binding_deduplicated_before_offer(tmp_path, monkeypatch):
    port, now = Port(), [100.0]
    monkeypatch.setattr(margin_trading.time, "monotonic", lambda: now[0])
    runtime = MarginTradingService(
        tmp_path, None, lambda: None, core_epoch="core", external_feature_provider=port
    )
    config, bar, binding = inputs()
    assert runtime._waiting_external("account", config, (bar,)) is True
    assert port.calls == [(binding, bar)]
    now[0] += 0.999
    assert runtime._waiting_external("account", config, (bar,)) is True
    now[0] += 0.001
    assert runtime._waiting_external("account", config, (bar,)) is False
    assert runtime._external_wait == {}


@pytest.mark.parametrize("disposition", ["ready", "no_signal", "expired", "unavailable"])
def test_known_delivery_or_failure_proceeds_to_financial_protection_without_grace(
    tmp_path, disposition
):
    port = Port()
    runtime = MarginTradingService(
        tmp_path, None, lambda: None, core_epoch="core", external_feature_provider=port
    )
    config, bar, _ = inputs()
    port.disposition = disposition
    assert runtime._waiting_external("account", config, (bar,)) is False
    port.failure = True
    assert runtime._waiting_external("account", config, (bar,)) is False
    assert runtime._external_wait == {}


def test_existing_pending_financial_frame_never_offers_or_waits_for_inference(tmp_path):
    port = Port()
    runtime = MarginTradingService(
        tmp_path,
        None,
        lambda: None,
        core_epoch="core",
        external_feature_provider=port,
        clock_ms=lambda: 120000,
    )
    calls = []
    config, _, _ = inputs()
    view = {"next_open_ms": 0, "instruments": [BTC.key], "pending_frame": True}
    session = SimpleNamespace(
        snapshot=lambda: view, run_once=lambda *args, **kwargs: calls.append((args, kwargs))
    )
    runtime.sessions["account"] = session
    runtime.data = {"accounts": {"account": {"feed_enabled": True, "parameters": config}}}
    runtime._tick()
    assert port.calls == [] and runtime.source is None
    assert len(calls) == 1 and calls[0][0] == ((),)
    assert runtime._external_wait == {}


def test_no_external_configuration_preserves_old_factory_options(tmp_path):
    runtime = MarginTradingService(tmp_path, None, lambda: None, core_epoch="core")
    assert runtime._feature_options({"strategies": [{"id": "legacy"}]}) == {}
    assert runtime._waiting_external("legacy", {"strategies": []}, ()) is False


@pytest.mark.parametrize("value", [False, True, -1, 30001, 1.5, "1000"])
def test_invalid_external_wait_cannot_be_accepted_as_a_configuration(value):
    with pytest.raises(margin_trading.ValidationError):
        margin_trading.external_feature_wait_ms({"external_feature_wait_ms": value})


def test_default_grace_covers_storage_latency_but_urgent_work_never_waits(tmp_path, monkeypatch):
    port, now = Port(), [100.0]
    monkeypatch.setattr(margin_trading.time, "monotonic", lambda: now[0])
    runtime = MarginTradingService(
        tmp_path, None, lambda: None, core_epoch="core", external_feature_provider=port
    )
    config, bar, _ = inputs()
    del config["external_feature_wait_ms"]
    assert margin_trading.external_feature_wait_ms(config) == 15000
    assert runtime._waiting_external("a", config, (bar,))
    now[0] += 6
    assert runtime._waiting_external("a", config, (bar,))
    assert not runtime._waiting_external("a", config, (bar,), immediate_protection=True)
    assert runtime._external_wait == {} and len(port.calls) == 3
    config["external_feature_wait_ms"] = 0
    assert not runtime._waiting_external("a", config, (bar,))
    assert runtime._external_wait == {}
