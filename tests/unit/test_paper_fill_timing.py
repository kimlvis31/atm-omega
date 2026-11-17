"""Paper fill event times and causal local protection, with deterministic feature input."""

from decimal import Decimal

import pytest

from omega.application.paper import PaperTradingService
from omega.application.paper_state import validate_state
from omega.ports.modules import ModuleDescriptor
from omega.strategies.registry import StrategyRegistry


class FixedFeatures:
    descriptor = ModuleDescriptor("fixed-test-input", "1", frozenset(), False)

    def __init__(self, value):
        self.value = value

    def update(self, _candle):
        return {"0_MMACD_MSDELTAABSMAREL": self.value}


def create(*, short=False, kind="MARKET", protection="fullStopLossImmediate"):
    paper = PaperTradingService(
        StrategyRegistry().create, lambda *_: FixedFeatures(-1.0 if short else 1.0)
    )
    paper.command("paper.account.create", {"account_id": "a", "balance": "10000"})
    configuration = {"order_type": kind}
    if protection is not None:
        configuration[protection] = "0.05"
    paper.command(
        "paper.strategy.add",
        {
            "instance": "tef",
            "account_id": "a",
            "instrument": "binance:usdm:BTCUSDT",
            "module_id": "tef",
            "budget": "100",
            "configuration": configuration,
        },
    )
    paper.command("resume", {})
    step(paper, 0)
    assert paper.detail("tef")["pending"]["eligible_at_ms"] == 60000
    paper.command(
        "pause", {}
    )  # Previously submitted entries stay active; protection is independent.
    return paper


def step(paper, minute, *, opening="100", high="101", low="99", close="100"):
    return paper.command(
        "paper.step",
        {
            "instrument": "binance:usdm:BTCUSDT",
            "candle": {
                "open_time_ms": minute * 60000,
                "open": opening,
                "high": high,
                "low": low,
                "close": close,
                "volume": "10",
            },
        },
    )


@pytest.mark.parametrize("short", [False, True])
@pytest.mark.parametrize("kind,late", [("MARKET", False), ("LIMIT", False), ("LIMIT", True)])
def test_known_open_fills_use_open_timestamp_but_later_limits_keep_unknown_intraminute_time(
    short, kind, late
):
    paper = create(short=short, kind=kind, protection=None)
    opening = ("90" if short else "110") if late else "100"
    step(paper, 1, opening=opening, high="112", low="88")
    detail = paper.detail("tef")
    assert len(detail["fills"]) == 1
    assert (
        detail["last_changed_ms"]
        == detail["fills"][0]["timestamp_ms"]
        == (120000 if late else 60000)
    )
    assert Decimal(detail["fills"][0]["price"]) == 100
    assert detail["pending"] is None
    validate_state(paper.data)


@pytest.mark.parametrize("short", [False, True])
def test_marketable_limit_price_improvement_also_has_a_known_open_event(short):
    paper = create(short=short, kind="LIMIT", protection=None)
    opening = "110" if short else "90"
    step(paper, 1, opening=opening, high="112", low="88")
    detail = paper.detail("tef")
    assert detail["last_changed_ms"] == detail["fills"][0]["timestamp_ms"] == 60000
    assert Decimal(detail["fills"][0]["price"]) == Decimal(opening)
    validate_state(paper.data)


@pytest.mark.parametrize("short", [False, True])
def test_limit_without_a_touch_keeps_the_submitted_order_and_has_no_fill_timestamp(short):
    paper = create(short=short, kind="LIMIT", protection=None)
    before = paper.detail("tef")["pending"]
    step(
        paper,
        1,
        opening="90" if short else "110",
        high="99" if short else "112",
        low="88" if short else "101",
        close="90" if short else "110",
    )
    detail = paper.detail("tef")
    assert not detail["fills"] and detail["pending"] == before
    assert detail["last_changed_ms"] == 0
    validate_state(paper.data)


@pytest.mark.parametrize("short", [False, True])
@pytest.mark.parametrize("kind,late", [("MARKET", False), ("LIMIT", False), ("LIMIT", True)])
def test_entry_bar_wick_is_only_eligible_when_position_existed_at_its_open(short, kind, late):
    paper = create(short=short, kind=kind)
    opening = ("90" if short else "110") if late else "100"
    step(paper, 1, opening=opening, high="112", low="88")
    detail = paper.detail("tef")
    assert len(detail["fills"]) == 1  # Protection decided at close cannot fill retrospectively.
    if late:
        assert detail["pending"] is None  # Unknown pre-entry wick, recovered close.
    else:
        assert detail["pending"]["reason"] == "FSLIMMED"
        assert detail["pending"]["reducing"]
        assert detail["pending"]["eligible_at_ms"] == 120000
    assert paper.data["paused"]
    validate_state(paper.data)


@pytest.mark.parametrize("short", [False, True])
@pytest.mark.parametrize("hit", [False, True])
def test_touched_limit_can_use_its_observed_close_without_claiming_its_full_pre_entry_wick(
    short, hit
):
    paper = create(short=short, kind="LIMIT", protection="fullStopLossClose")
    close = ("105" if short else "95") if hit else "100"
    step(paper, 1, opening="90" if short else "110", high="112", low="88", close=close)
    detail = paper.detail("tef")
    assert detail["last_changed_ms"] == 120000
    assert len(detail["fills"]) == 1
    if hit:
        assert detail["pending"]["reason"] == "FSLCLOSE"
        assert detail["pending"]["reducing"]
        assert detail["pending"]["eligible_at_ms"] == 120000
    else:
        assert detail["pending"] is None
    validate_state(paper.data)
