"""The new bounded calendar display endpoint preserves owner-only controls."""

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.service import CoreService


@pytest.fixture
def core(tmp_path):
    value = CoreService(tmp_path)
    yield value
    value.close()


def test_calendar_query_dispatch_and_authentication_are_independent_of_minute_query(core):
    calls = []
    core.calendar_candles_provider = lambda *args: calls.append(args) or {"candles": []}
    assert core.state()["core"]["capabilities"]["calendar_bars"]
    with pytest.raises(PermissionError):
        core.request("GET", "/api/chart-candles?period=1M", "wrong")
    assert not calls
    core.request(
        "GET",
        "/api/chart-candles?instrument=binance:spot:ETHUSDT&period=1M&limit=12&end_ms=120000",
        core.token,
    )
    assert calls == [("binance:spot:ETHUSDT", "1M", 12, 120000)]
    assert core.request("GET", "/api/candles", core.token)["candles"] == []


@pytest.mark.parametrize(
    "query", ["limit=bad", "end_ms=1.5", "period=1w&period=1M", "limit=", "injected=yes"]
)
def test_bad_query_does_not_reach_calendar_database_provider(core, query):
    core.calendar_candles_provider = lambda *_: pytest.fail("invalid query reached provider")
    with pytest.raises(ValidationError):
        core.request("GET", "/api/chart-candles?" + query, core.token)
