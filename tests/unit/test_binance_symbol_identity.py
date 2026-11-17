"""Captured public Unicode ticker identities and fail-closed per-row catalog parsing."""

import json
from copy import deepcopy
from hashlib import sha256

import pytest

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.execution_rules import ExecutionRulesSnapshot
from omega.domain.models import Instrument, Market, is_valid_symbol
from omega.infrastructure.binance.execution_rules import rules_snapshot
from omega.infrastructure.binance.market import BinanceMarketData

# Five identifiers from the preserved public USD-M exchangeInfo response,
# SHA256 e89230b75fe8050a978f4a459e903f5330354e702d1f20467845b7a702f79b18.
PUBLIC_UNICODE_SYMBOLS = [
    "\u5e01\u5b89\u4eba\u751fUSDT",
    "\u6211\u8e0f\u9a6c\u6765\u4e86USDT",
    "\u9f99\u867eUSDT",
    "\u725b\u6765USDT",
    "\u54c8\u57fa\u7c73USDT",
]


def row(symbol="BTCUSDT", *, contract="PERPETUAL"):
    return {
        "symbol": symbol,
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "status": "TRADING",
        "contractType": contract,
        "filters": [
            {"filterType": "LOT_SIZE", "minQty": ".001", "maxQty": "100", "stepSize": ".001"},
            {"filterType": "PRICE_FILTER", "minPrice": "0", "maxPrice": "10000", "tickSize": ".01"},
        ],
    }


class Client:
    def __init__(self, payload):
        self.payload, self.calls = payload, []

    def request(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        return self.payload


def adapter(payload, market=Market.USDM):
    return BinanceMarketData(market, Client(payload))


@pytest.mark.parametrize("symbol", PUBLIC_UNICODE_SYMBOLS)
def test_actual_captured_unicode_identifiers_roundtrip_without_normalizing_identity(symbol):
    assert is_valid_symbol(symbol)
    instrument = Instrument("binance", Market.USDM, symbol)
    assert instrument.symbol == symbol and instrument.key == "binance:usdm:" + symbol
    market = adapter({"symbols": [row(symbol)]})
    normalized = rules_snapshot(market.symbols()[0], observed_at_ms=900100)
    assert ExecutionRulesSnapshot.from_document(normalized.document()) == normalized
    assert market.last_symbol_diagnostics["invalid_rows"] == 0


@pytest.mark.parametrize("symbol", ["BTCUSDT", "ETH-USDT", "BTC_USDT", "__", "1234", "A" * 40])
def test_old_ascii_identifiers_and_key_hashes_remain_unchanged(symbol):
    instrument = Instrument("binance", Market.SPOT, symbol)
    original = f"binance:spot:{symbol}"
    assert instrument.key == original
    assert sha256(instrument.key.encode()).hexdigest() == sha256(original.encode()).hexdigest()


@pytest.mark.parametrize(
    "symbol",
    [
        None,
        True,
        1,
        b"BTCUSDT",
        "A",
        "A" * 41,
        "btcUSDT",
        "ＢＴＣUSDT",
        "𝐁TCUSDT",
        "BTC:USDT",
        "BTC/USDT",
        "BTC\\USDT",
        "BTC USDT",
        "BTC\nUSDT",
        "BTC\x00USDT",
        "BTC\u202eUSDT",
        "BTC\u200dUSDT",
        "币安\ufe0fUSDT",
        "币安\u0301USDT",
        "אבUSDT",
        "عملةUSDT",
        "١USDT",
    ],
)
def test_unsafe_confusable_width_control_path_or_bidi_identifiers_are_rejected(symbol):
    assert not is_valid_symbol(symbol)
    with pytest.raises(ValidationError):
        Instrument("binance", Market.USDM, symbol)


def test_one_bad_row_does_not_discard_good_ascii_or_unicode_and_contract_exclusions_are_explicit():
    market = adapter(
        {
            "symbols": [
                row(),
                None,
                row("BTC/USDT"),
                row(PUBLIC_UNICODE_SYMBOLS[0]),
                row("SYNTHUSDT", contract="TRADIFI_PERPETUAL"),
                row("BTCUSDT_261225", contract="CURRENT_QUARTER"),
            ]
        }
    )
    returned = market.symbols()
    assert {item.instrument.symbol for item in returned} == {"BTCUSDT", PUBLIC_UNICODE_SYMBOLS[0]}
    diagnostic = market.last_symbol_diagnostics
    assert diagnostic["accepted"] == diagnostic["invalid_rows"] == 2
    assert diagnostic["excluded_contracts"] == {"TRADIFI_PERPETUAL": 1, "CURRENT_QUARTER": 1}
    assert market.client.calls == [("GET", "/fapi/v1/exchangeInfo", {"weight": 1})]


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"symbols": None},
        {"symbols": {}},
        {"symbols": []},
        {"symbols": [None] * 10001},
    ],
)
def test_top_level_invalid_or_oversized_catalog_is_not_a_successful_empty(payload):
    market = adapter(payload)
    with pytest.raises(ProtocolError):
        market.symbols()
    assert market.last_symbol_diagnostics["state"] == "invalid_catalog"


def test_all_invalid_or_all_unsupported_contracts_fail_with_bounded_diagnostics():
    for rows in ([None] * 100, [row("SYNTHUSDT", contract="TRADIFI_PERPETUAL")]):
        market = adapter({"symbols": rows})
        with pytest.raises(ProtocolError):
            market.symbols()
        diagnostic = market.last_symbol_diagnostics
        assert diagnostic["state"] == "no_supported_symbols" and diagnostic["accepted"] == 0
        assert len(diagnostic["examples"]) <= 8 and len(json.dumps(diagnostic)) < 4096


@pytest.mark.parametrize("first_invalid", [False, True])
def test_duplicate_identity_quarantines_both_rows_including_an_earlier_malformed_row(first_invalid):
    first = row()
    if first_invalid:
        first["baseAsset"] = None
    market = adapter({"symbols": [first, row(), row("ETHUSDT")]})
    returned = market.symbols()
    assert [item.instrument.symbol for item in returned] == ["ETHUSDT"]
    assert market.last_symbol_diagnostics["invalid_rows"] == 2


def test_only_duplicate_identities_are_not_reported_as_an_empty_success():
    with pytest.raises(ProtocolError):
        adapter({"symbols": [row(), row()]}).symbols()


def test_invalid_row_diagnostics_are_detached_and_never_publish_raw_control_text():
    payload = {"symbols": [row("BAD\u202eUSDT"), row()]}
    market = adapter(payload)
    returned = market.symbols()
    diagnostic = market.last_symbol_diagnostics
    assert "BAD" not in str(diagnostic) and "\u202e" not in str(diagnostic)
    diagnostic["examples"].clear()
    payload["symbols"][1]["filters"][0]["stepSize"] = "corrupted external dict"
    assert market.last_symbol_diagnostics["examples"]
    assert returned[0].filters[0]["stepSize"] == ".001"


def test_spot_keeps_its_original_contract_independent_behavior():
    payload = {"symbols": [row(PUBLIC_UNICODE_SYMBOLS[0], contract="UNRELATED_SPOT_FIELD")]}
    market = adapter(deepcopy(payload), Market.SPOT)
    assert market.symbols()[0].instrument.market == Market.SPOT
    assert market.last_symbol_diagnostics["excluded_contracts"] == {}
