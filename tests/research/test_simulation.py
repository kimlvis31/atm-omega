from decimal import Decimal as D

import pytest

from omega.research.datasets import MarketRow
from omega.research.simulation import OrderRequest, SimulationSpec, candle_path, simulate


def candle(index, prices=(100, 110, 90, 100), instrument="binance:usdm:BTCUSDT"):
    return MarketRow(instrument, index * 60000, *(D(value) for value in prices), D(100))


def test_orders_cannot_fill_past_signal_and_latency_crossing():
    calls = []

    def strategy(view):
        calls.append(view)
        return [OrderRequest("binance:usdm:BTCUSDT", D(1))] if len(calls) == 1 else []

    result = simulate(
        [candle(0), candle(1)], strategy, SimulationSpec(fee_rate=D(0), latency_ms=10000)
    )
    assert len(result["fills"]) == 1
    assert D(result["fills"][0]["timestamp_ms"]) == 70000
    assert D(result["fills"][0]["price"]) == 105
    assert len(calls[0].history) == 1
    assert calls[0].timestamp_ms == 60000
    assert calls[0].positions["binance:usdm:BTCUSDT"] == 0


def test_exit_order_created_at_close_does_not_use_same_bar_low():
    def strategy(view):
        if view.timestamp_ms == 60000:
            return [OrderRequest("binance:usdm:BTCUSDT", D(1))]
        if view.timestamp_ms == 120000:
            return [OrderRequest("binance:usdm:BTCUSDT", D(-1), "stop", D(95), True)]
        return []

    result = simulate(
        [candle(0), candle(1), candle(2, (100, 110, 99, 105))],
        strategy,
        SimulationSpec(fee_rate=D(0)),
    )
    assert len(result["fills"]) == 1
    assert result["pending_orders"] == 1


def test_path_choice_resolves_oco_and_reports_ambiguity():
    def strategy(view):
        if view.timestamp_ms == 60000:
            return [OrderRequest("binance:usdm:BTCUSDT", D(1))]
        if view.timestamp_ms == 120000:
            return [
                OrderRequest("binance:usdm:BTCUSDT", D(-1), "stop", D(95), True, "sl", "protect"),
                OrderRequest("binance:usdm:BTCUSDT", D(-1), "limit", D(105), True, "tp", "protect"),
            ]
        return []

    rows = [candle(0), candle(1), candle(2)]
    high_first = simulate(rows, strategy, SimulationSpec(fee_rate=D(0), path="O-H-L-C"))
    low_first = simulate(rows, strategy, SimulationSpec(fee_rate=D(0), path="O-L-H-C"))
    assert high_first["fills"][-1]["tag"] == "tp"
    assert low_first["fills"][-1]["tag"] == "sl"
    assert high_first["wallet"] == "10005"
    assert low_first["wallet"] == "9995"
    assert any(event["kind"] == "intrabar_ambiguity" for event in high_first["events"])


def test_seed_path_is_shared_and_supports_case_overrides_and_flat():
    row = candle(0)
    spec = SimulationSpec(path="random", seed=42)
    assert candle_path(row, spec) == candle_path(row, spec)
    path = candle_path(row, spec)
    assert [price for _, price in path][0] == row.open
    assert max(price for _, price in path) == row.high
    assert min(price for _, price in path) == row.low
    assert all(a[0] < b[0] for a, b in zip(path, path[1:], strict=False))
    flat = candle(0, (100, 100, 100, 100))
    assert all(price == 100 for _, price in candle_path(flat, spec))
    custom = SimulationSpec(routes={"doji": "O-L-H-C"})
    assert candle_path(row, custom)[1][1] == 90
    with pytest.raises(ValueError):
        SimulationSpec(path="random", routes={"bullish": "O-H-L-C"})


def test_costs_funding_reduction_and_reversal_accounting():
    def strategy(view):
        if view.timestamp_ms == 60000:
            return [OrderRequest("binance:usdm:BTCUSDT", D(2))]
        if view.timestamp_ms == 120000:
            return [OrderRequest("binance:usdm:BTCUSDT", D(-3))]
        if view.timestamp_ms == 180000:
            return [OrderRequest("binance:usdm:BTCUSDT", D(5), reduce_only=True)]
        return []

    spec = SimulationSpec(
        fee_rate=D("0.01"),
        slippage_rate=D("0.01"),
        funding=({"timestamp_ms": 120000, "instrument": "binance:usdm:BTCUSDT", "rate": "0.01"},),
    )
    result = simulate([candle(index) for index in range(4)], strategy, spec)
    assert [D(fill["quantity"]) for fill in result["fills"]] == [2, -3, 1]
    assert result["positions"]["binance:usdm:BTCUSDT"] == "0"
    assert sum(D(fill["realized_pnl"]) for fill in result["fills"]) == -6
    assert sum(D(fill["fee"]) for fill in result["fills"]) == 6
    assert D(result["wallet"]) == 9986


def test_shared_capital_does_not_allow_two_overallocations():
    def strategy(view):
        return (
            [OrderRequest(row.instrument, D(80)) for row in view.candles]
            if view.timestamp_ms == 60000
            else []
        )

    rows = [candle(index, instrument=instrument) for index in range(2) for instrument in ("A", "B")]
    result = simulate(rows, strategy, SimulationSpec(fee_rate=D(0), backend="gpu"))
    assert len(result["fills"]) == 1
    assert result["backend"]["selected"] == "cpu"
    assert "GPU" in result["backend"]["reason"]
    assert any(event["kind"] == "insufficient_unleveraged_capital" for event in result["events"])


def test_funding_spot_or_duplicates_rejected():
    event = {"timestamp_ms": 0, "instrument": "binance:spot:BTCUSDT", "rate": "0.01"}
    with pytest.raises(ValueError, match="derivatives"):
        simulate(
            [candle(0, instrument="binance:spot:BTCUSDT")],
            lambda view: [],
            SimulationSpec(funding=(event,)),
        )
    event = event | {"instrument": "binance:usdm:BTCUSDT"}
    with pytest.raises(ValueError, match="duplicate funding"):
        simulate([candle(0)], lambda view: [], SimulationSpec(funding=(event, event)))


def test_legacy_costs_are_distinct_and_do_not_claim_full_compatibility():
    def strategy(view):
        return [OrderRequest("binance:usdm:BTCUSDT", D(1))] if view.timestamp_ms == 60000 else []

    result = simulate([candle(0), candle(1)], strategy, SimulationSpec(profile="legacy_costs"))
    assert D(result["fills"][0]["price"]) == D("100.15")
    assert D(result["fills"][0]["fee"]) == D("0.050075")
    assert any("not full Eta" in assumption for assumption in result["assumptions"])


def test_common_ts_family_runs_without_tef_assumptions():
    from omega.research.strategy_binding import TradeStrategyBinding
    from omega.strategies.target_quantity.module import create

    strategy = TradeStrategyBinding(create, {"quantity": "2"})
    result = simulate(
        [candle(index) for index in range(3)], strategy, SimulationSpec(fee_rate=D(0))
    )
    assert len(result["fills"]) == 1
    assert result["positions"]["binance:usdm:BTCUSDT"] == "2"
    assert strategy.engines["binance:usdm:BTCUSDT"].module.snapshot()["module"] == "target_quantity"


def test_gap_cannot_bypass_common_ts_allocation_budget():
    from omega.research.strategy_binding import TradeStrategyBinding
    from omega.strategies.target_quantity.module import create

    strategy = TradeStrategyBinding(create, {"quantity": "10"}, allocated_per_instrument=D(1000))
    rows = [candle(0, (100, 100, 100, 100)), candle(1, (200, 200, 200, 200))]
    result = simulate(rows, strategy, SimulationSpec(fee_rate=D(0)))
    assert not result["fills"]
    assert any(event["kind"] == "strategy_budget_exceeded_after_gap" for event in result["events"])


def test_spot_short_rejected_and_callback_history_bounded():
    history_sizes = []

    def strategy(view):
        history_sizes.append(len(view.history))
        return [OrderRequest("binance:spot:BTCUSDT", D(-1))] if view.timestamp_ms == 60000 else []

    result = simulate(
        [candle(index, instrument="binance:spot:BTCUSDT") for index in range(5)],
        strategy,
        SimulationSpec(history_rows=2),
    )
    assert not result["fills"]
    assert any(event["kind"] == "short_not_supported" for event in result["events"])
    assert history_sizes == [1, 2, 2, 2, 2]


def test_tef_binding_requires_declared_features_and_advances_while_pending():
    from omega.research.strategy_binding import TradeStrategyBinding, build_features
    from omega.strategies.tef.module import create

    missing = TradeStrategyBinding(create)
    with pytest.raises(ValueError, match="does not provide"):
        simulate([candle(0)], missing, SimulationSpec())
    config = {"moving_averages": [{"interval_id": 0, "mmacd_periods": [2, 3], "signal_period": 2}]}
    strategy = TradeStrategyBinding(
        create, feature_factory=lambda instrument: build_features(instrument, config)
    )
    rows = [candle(index, (100 + index % 5, 120, 80, 100 + (index + 1) % 5)) for index in range(25)]
    result = simulate(rows, strategy, SimulationSpec(latency_ms=120000))
    assert result["state"] == "complete"
    assert (
        strategy.engines["binance:usdm:BTCUSDT"].module.model._last_candle.open_time_ms
        == 24 * 60000
    )


def test_mixed_quote_market_data_rejected_before_strategy_evaluation():
    calls = []
    rows = [candle(0), candle(0, instrument="binance:spot:ETHBTC")]
    with pytest.raises(ValueError, match="explicit quote_assets"):
        simulate(rows, lambda view: calls.append(view) or [], SimulationSpec())
    with pytest.raises(ValueError, match="mixed quote currencies"):
        simulate(
            rows,
            lambda view: calls.append(view) or [],
            SimulationSpec(
                quote_assets={
                    "binance:spot:ETHBTC": "BTC",
                }
            ),
        )
    assert not calls


def test_single_non_usdt_market_requires_and_uses_explicit_quote_unit():
    instrument = "binance:spot:ETHBTC"

    def strategy(view):
        if view.timestamp_ms == 60000:
            return [OrderRequest(instrument, D(1))]
        if view.timestamp_ms == 120000:
            return [OrderRequest(instrument, D(-1), reduce_only=True)]
        return []

    spec = SimulationSpec(
        initial_wallet=D(10), fee_rate=D(0), quote_asset="BTC", quote_assets={instrument: "BTC"}
    )
    rows = [
        candle(index, (price,) * 4, instrument)
        for index, price in enumerate(("0.05", "0.05", "0.06"))
    ]
    result = simulate(rows, strategy, spec)
    assert result["quote_asset"] == "BTC"
    assert result["quote_assets"] == {instrument: "BTC"}
    assert D(result["wallet"]) == D("10.01")
    assert any("funding use BTC" in assumption for assumption in result["assumptions"])


def test_unknown_or_contradictory_quote_metadata_is_rejected():
    with pytest.raises(ValueError, match="unknown dataset instrument"):
        simulate([candle(0)], lambda view: [], SimulationSpec(quote_assets={"TYPO": "USDT"}))
    with pytest.raises(ValueError, match="contradicts"):
        simulate(
            [candle(0)],
            lambda view: [],
            SimulationSpec(
                quote_asset="BTC",
                quote_assets={
                    "binance:usdm:BTCUSDT": "BTC",
                },
            ),
        )
    with pytest.raises(ValueError, match="explicit quote_assets"):
        simulate([candle(0, instrument="other:spot:BTCUSDT")], lambda view: [], SimulationSpec())


@pytest.mark.parametrize(
    "configuration",
    [
        {"quote_asset": ""},
        {"quote_asset": "usd"},
        {"quote_asset": 1},
        {"quote_assets": []},
        {"quote_assets": None},
        {"quote_assets": {1: "USDT"}},
        {"quote_assets": {"A": None}},
        {"quote_assets": {"A": ""}},
        {"quote_assets": {"A": "btc"}},
    ],
)
def test_quote_metadata_bad_types_and_currency_ids(configuration):
    with pytest.raises(ValueError, match="quote_asset"):
        SimulationSpec.parse(configuration)


def test_synthetic_ids_explicitly_use_the_configured_quote_unit():
    result = simulate(
        [candle(0, instrument="A"), candle(0, instrument="B")],
        lambda view: [],
        SimulationSpec(quote_asset="KRW"),
    )
    assert result["quote_assets"] == {"A": "KRW", "B": "KRW"}
    assert any("synthetic series" in assumption for assumption in result["assumptions"])
