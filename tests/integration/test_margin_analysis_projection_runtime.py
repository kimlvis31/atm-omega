"""Real virtual fills/restart and immutable full CA under explicit dependency selection."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256

import pytest

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.margin import FillEvent, MarginBracket, MarginBracketSnapshot
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.margin_session import MarginTradingSession
from omega.strategies.registry import StrategyRegistry

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
POLICY = {"version": 1, "policy": "required_fields"}


def configuration(*, projected):
    account = {
        "account_id": "projection-real-files",
        "balance": "1000",
        "positions": [
            {
                "instrument": BTC.key,
                "owner_id": "strategy",
                "margin_mode": "cross",
                "leverage": 2,
                "budget_margin": "100",
            }
        ],
    }
    analysis = {
        "moving_averages": [
            {"interval_id": 0, "ema_period": None, "mmacd_periods": [2, 3], "signal_period": 2}
        ]
    }
    row = {
        "id": "strategy",
        "instrument": BTC.key,
        "module": "tef",
        "configuration": {"fullStopLossImmediate": ".05"},
        "analysis": analysis,
        "constraints": {"leverage": "2", "can_short": True},
    }
    if projected:
        analysis["moving_averages"][0]["ema_period"] = 1000
        analysis["market_sentiment"] = [
            {"interval_id": 0, "nSamples": 2, "maximum_metric_age_ms": 330000}
        ]
        analysis["order_flow"] = [{"interval_id": 0, "analysis_code": "WOI", "nSamples": 2}]
        row["analysis_execution"] = deepcopy(POLICY)
    return account, [row]


def bar(index):
    previous = D(100) + D(max(0, index - 1)) / 100
    close = D(100) + D(index) / 100
    return Candle(
        BTC,
        index * 60000,
        (index + 1) * 60000,
        previous,
        close + D(".01"),
        previous - D(".01"),
        close,
        D(10),
    )


def brackets():
    return (
        MarginBracketSnapshot(
            BTC,
            (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),),
            0,
            0,
            "explicit_test_reference",
            "a" * 64,
        ),
    )


def fills(session):
    return [
        (row.fact.delta_qty, row.fact.price, row.fact.fee, row.fact.event_time_ms)
        for row in session.execution.snapshot().account.ledger
        if isinstance(row.fact, FillEvent)
    ]


def hashes(root):
    return {
        p.relative_to(root).as_posix(): sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*.json")
        if p.is_file()
    }


def test_full_unused_ca_matches_minimal_real_trading_and_restarts_without_new_raw_inputs(tmp_path):
    minimal_account, minimal_rows = configuration(projected=False)
    account, rows = configuration(projected=True)
    before = deepcopy(rows)
    actual_root = tmp_path / "projected"
    minimal = MarginTradingSession(
        tmp_path / "minimal", minimal_account, minimal_rows, 0, create=True
    )
    actual = MarginTradingSession(actual_root, account, rows, 0, create=True)
    try:
        assert actual.analysis_input_channels == {} and actual.analysis_metric_policies == {}
        assert actual._schema_version == minimal._schema_version == 1
        assert "analysis_execution" not in minimal._meta["configuration"]
        frozen = actual._meta["configuration"]["analysis_execution"]["strategy"]
        assert frozen["module_descriptor"]["required_fields"] == ["0_MMACD_MSDELTAABSMAREL"]
        assert actual._meta["configuration"]["strategies"] == before
        minimal.resume("explicit-start")
        actual.resume("explicit-start")
        for index in range(14):
            for session in (minimal, actual):
                session.run_once((bar(index),), as_of_ms=(index + 1) * 60000, brackets=brackets())
        expected = fills(minimal)
        assert expected and fills(actual) == expected
        assert (
            actual.execution.snapshot().account.cross_wallet
            == minimal.execution.snapshot().account.cross_wallet
        )
        assert rows == before
        metadata = json.loads(actual.path.read_bytes())["data"]
        assert metadata["configuration"]["strategies"][0]["analysis"] == before[0]["analysis"]
    finally:
        minimal.close()
        actual.close()
    reopened = MarginTradingSession(actual_root, account, rows, 0, create=False)
    try:
        assert reopened.snapshot()["paused"]
        assert fills(reopened) == expected and reopened.analysis_input_channels == {}
        reopened.run_once((bar(13),), as_of_ms=840000, brackets=brackets())
        assert fills(reopened) == expected
    finally:
        reopened.close()


@pytest.mark.parametrize("change", ["unused_profile", "remove_policy", "descriptor"])
def test_changed_full_profile_policy_or_actual_ts_descriptor_cannot_adopt_prior_state(
    tmp_path, change
):
    account, rows = configuration(projected=True)
    root = tmp_path / "persisted"
    session = MarginTradingSession(root, account, rows, 0, create=True)
    session.close()
    before = hashes(root)
    registry = None
    if change == "unused_profile":
        rows[0]["analysis"]["market_sentiment"][0]["nSamples"] = 3
    elif change == "remove_policy":
        del rows[0]["analysis_execution"]
    else:

        class ChangedDescriptorRegistry:
            def create(self, *args):
                module = StrategyRegistry().create(*args)
                module.descriptor = replace(module.descriptor, api_version="2")
                return module

        registry = ChangedDescriptorRegistry()
    with pytest.raises((ProtocolError, StorageError, ValidationError)):
        MarginTradingSession(root, account, rows, 0, create=False, registry=registry)
    assert hashes(root) == before
