"""Opt-in raw candle epoch evidence cannot relabel an old owner's response."""

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.service import CoreService


def test_unflagged_candle_response_keeps_legacy_shape_and_provider_object(tmp_path):
    packet = {"instrument": "binance:usdm:BTCUSDT", "candles": [], "source": "database"}
    core = CoreService(tmp_path, candles_provider=lambda instrument, limit: packet)
    try:
        assert core.request("GET", "/api/candles", core.token) is packet
        assert "core_epoch" not in packet
        exact = core.request("GET", "/api/candles?include_core_epoch=1", core.token)
        assert exact == packet | {"core_epoch": core.control.epoch}
        assert "core_epoch" not in packet
    finally:
        core.close()


def test_epoch_is_captured_before_provider_and_provider_cannot_spoof_the_owner(tmp_path):
    core = CoreService(tmp_path)
    initial = core.control.epoch
    packet = {"instrument": "binance:usdm:BTCUSDT", "candles": [], "core_epoch": "spoofed"}

    def provider(instrument, limit):
        # A delayed response belongs to the owner that admitted it. A GUI may
        # already have observed a new owner while the old reply is in transit.
        core.control.epoch = "b" * 32
        return packet

    core.candles_provider = provider
    try:
        result = core.request("GET", "/api/candles?include_core_epoch=1", core.token)
        assert result["core_epoch"] == initial
        assert result["core_epoch"] != core.control.epoch
        assert packet["core_epoch"] == "spoofed"
    finally:
        core.control.epoch = initial
        core.close()


@pytest.mark.parametrize("option", ["", "true", "0", "1&include_core_epoch=1"])
def test_invalid_epoch_option_is_rejected_before_provider_read(tmp_path, option):
    calls = []
    core = CoreService(tmp_path, candles_provider=lambda *args: calls.append(args))
    try:
        with pytest.raises(ValidationError):
            core.request("GET", "/api/candles?include_core_epoch=" + option, core.token)
        assert calls == []
    finally:
        core.close()


def test_unavailable_candles_remain_explicit_with_an_opt_in_epoch(tmp_path):
    core = CoreService(tmp_path)
    try:
        result = core.request("GET", "/api/candles?include_core_epoch=1", core.token)
        assert result["core_epoch"] == core.control.epoch
        assert result["candles"] == [] and result["quality"]["status"] == "unavailable"
    finally:
        core.close()
