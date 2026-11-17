"""Never mix instruments quoted in different assets into one USDT paper wallet."""

import pytest

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService
from omega.application.paper_state import validate_state
from omega.domain.errors import ValidationError
from omega.strategies.registry import StrategyRegistry


@pytest.mark.parametrize("symbol", ["ETHBTC", "BTCUSDC", "BTCEUR"])
def test_non_usdt_strategy_is_rejected_before_any_cash_is_committed(symbol):
    service = PaperTradingService(StrategyRegistry().create, create_features)
    service.command("paper.account.create", {"account_id": "a"})
    with pytest.raises(ValidationError, match="USDT"):
        service.command(
            "paper.strategy.add",
            {
                "instance": "x",
                "account_id": "a",
                "module_id": "target_quantity",
                "instrument": f"binance:spot:{symbol}",
                "budget": "100",
                "configuration": {"quantity": "1"},
            },
        )
    assert service.data["strategies"] == {}
    assert service.data["accounts"]["a"]["balance"] == "10000"
    assert service.snapshot()["accounts"][0]["quote_asset"] == "USDT"


def test_corrupt_recovery_cannot_relabel_a_position_into_another_settlement_asset():
    service = PaperTradingService(StrategyRegistry().create, create_features)
    service.command("paper.account.create", {"account_id": "a"})
    service.command(
        "paper.strategy.add",
        {
            "instance": "x",
            "account_id": "a",
            "module_id": "target_quantity",
            "instrument": "binance:spot:ETHUSDT",
            "budget": "100",
            "configuration": {"quantity": "1"},
        },
    )
    service.data["strategies"]["x"]["instrument"] = "binance:spot:ETHBTC"
    with pytest.raises(ValidationError, match="USDT"):
        validate_state(service.data)
