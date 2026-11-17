"""Actual isolated PG + core API + file-owned v2 TS, using synthetic test clocks.

The prices/reference table are explicit fixtures, not exchange observations or
live fills. External network remains blocked by the shared test boundary.
"""

import os
import time
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.analysis_frames import CommittedAnalysisFrameSource
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.ports.collection import MinuteObservation
from omega.runtime.margin_trading import MarginTradingService
from omega.runtime.service import CoreService
from omega.strategies.registry import StrategyRegistry

pytestmark = pytest.mark.database


def await_value(read, check, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = read()
        if check(value):
            return value
        time.sleep(0.02)
    raise AssertionError(value)


def operation(core, identity, kind, parameters):
    epoch = core.control.snapshot()["epoch"]
    core.request(
        "POST",
        "/api/margin-trading",
        core.token,
        {"id": identity, "kind": kind, "parameters": parameters, "core_epoch": epoch},
    )
    result = await_value(
        lambda: core.request("GET", "/api/margin-trading/" + identity, core.token),
        lambda r: r["state"] not in {"queued", "running"},
    )
    assert result["state"] == "completed", result
    return result


def global_control(core, kind, identity):
    state = core.control.snapshot()
    payload = (
        {}
        if kind == "pause"
        else {
            "expected_epoch": state["epoch"],
            "expected_revision": state["control_revision"],
            "expires_at_ms": int(time.time() * 1000) + 10000,
        }
    )
    response = core.request(
        "POST", "/api/commands", core.token, {"id": identity, "kind": kind, "payload": payload}
    )
    assert response["status"] == "completed", response


@pytest.mark.parametrize("costed", [False, True], ids=["legacy", "cost_policy"])
def test_actual_pg_to_automatic_strategy_pause_pending_fill_and_restart_without_duplicate(
    tmp_path,
    request,
    costed,
):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires --database-tests and isolated DB")
    config = Path(os.environ["OMEGA_TEST_DB_CONFIG"])
    settings = load_database_settings(config)
    assert (settings.host, settings.port, settings.dbname, settings.user) == (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    )
    (tmp_path / "database.config.toml").write_bytes(config.read_bytes())
    instrument = Instrument("binance", Market.USDM, "TESTMARG" + uuid4().hex[:16].upper() + "USDT")
    opening = 1_700_000_040_000
    now = [opening + 120001]
    store = PostgresCandleStore(settings)
    source_calls = []

    class ObservedSource(CommittedAnalysisFrameSource):
        def read(self, *args, **kwargs):
            source_calls.append((args, kwargs))
            return super().read(*args, **kwargs)

    def core_at_root():
        core = CoreService(tmp_path)
        core.margin_trading = MarginTradingService(
            tmp_path,
            StrategyRegistry(),
            lambda: ObservedSource(tmp_path),
            core_epoch=core.control.snapshot()["epoch"],
            clock_ms=lambda: now[0],
            mutation_lock=core.owner_mutation_lock,
            entry_gate=lambda: not core._closing and not core.control.snapshot()["paused"],
        )
        core.margin_trading.start()
        await_value(core.margin_trading.snapshot, lambda row: row["state"] == "ready")
        return core

    core = None
    try:
        for index in range(4):
            bar = Candle(
                instrument,
                opening + index * 60000,
                opening + (index + 1) * 60000,
                *(Decimal(v) for v in (100, 102, 99, 100, 10)),
            )
            store.put(MinuteObservation(bar, "rest", bar.close_time_ms))
        core = core_at_root()
        parameters = {
            "account": {
                "account_id": "pg-margin",
                "balance": "1000",
                "positions": [
                    {
                        "instrument": instrument.key,
                        "owner_id": "target",
                        "margin_mode": "cross",
                        "leverage": 10,
                        "budget_margin": "100",
                    }
                ],
            },
            "strategies": [
                {
                    "id": "target",
                    "instrument": instrument.key,
                    "module": "target_quantity",
                    "configuration": {"quantity": "1"},
                    "analysis": {},
                    "constraints": {"leverage": "10", "can_short": True},
                }
            ],
            "start_ms": opening,
            "fee_rate": "0.001",
            "brackets": [
                {
                    "instrument": instrument.key,
                    "observed_at_ms": opening,
                    "available_at_ms": opening,
                    "rows": [
                        {
                            "notional_floor": "0",
                            "notional_cap": "1000000",
                            "maintenance_rate": "0.01",
                            "maintenance_amount": "0",
                            "max_leverage": 20,
                        }
                    ],
                }
            ],
        }
        if costed:
            parameters["cost_policy"] = {
                "schema_version": 1,
                "model": "linear_usdt_ohlcv_cost_v1",
                "slippage_rate": "0.01",
                "execution_price_bound_rate": "0.1",
                "mark_drift_bound_rate": "0.1",
                "entry_delay_ms": 1,
                "reduction_delay_ms": 0,
                "limit_price_policy": "adverse_then_cap_v1",
            }
        operation(core, "configure", "configure", parameters)
        assert source_calls == []
        global_control(core, "resume", "global-resume")
        original_epoch = core.control.snapshot()["epoch"]
        operation(core, "resume-margin", "resume", {"account_id": "pg-margin"})
        view = await_value(
            lambda: core.margin_trading.snapshot()["accounts"][0],
            lambda row: row["cursor_ms"] == opening + 60000,
        )
        assert len(view["pending_orders"]) == 1 and view["account"]["summary"]["ledger_total"] == 0
        global_control(core, "pause", "global-pause")
        now[0] = opening + 240001
        view = await_value(
            lambda: core.margin_trading.snapshot()["accounts"][0],
            lambda row: row["cursor_ms"] == opening + 180000,
        )
        assert view["account"]["summary"]["ledger_total"] == 1 and view["pending_orders"] == []
        expected_wallet = Decimal("999.899" if costed else "999.9")
        assert Decimal(view["account"]["account"]["cross_wallet"]) == expected_wallet
        assert Decimal(view["account"]["account"]["positions"][0]["quantity"]) == 1
        assert Decimal(view["account"]["account"]["positions"][0]["entry_price"]) == (
            101 if costed else 100
        )
        if costed:
            assert view["cost_policy"]["scope"] == "fixed_adverse_full_fill_delay_v1"
        core.close()
        core = None
        reads = len(source_calls)
        core = core_at_root()
        restored = core.margin_trading.snapshot()["accounts"][0]
        assert restored["paused"] and restored["account"]["summary"]["ledger_total"] == 1
        assert Decimal(restored["account"]["account"]["cross_wallet"]) == expected_wallet
        assert len(source_calls) == reads
        cached = operation(core, "resume-margin", "resume", {"account_id": "pg-margin"})
        assert cached["operation_core_epoch"] == original_epoch
        assert core.margin_trading.snapshot()["accounts"][0]["paused"]
        assert len(source_calls) == reads
    finally:
        if core is not None:
            core.close()
        store.close()
        with connect_database(settings) as connection:
            connection.execute(
                "DELETE FROM market.candles_1m WHERE instrument=%s", (instrument.key,)
            )
