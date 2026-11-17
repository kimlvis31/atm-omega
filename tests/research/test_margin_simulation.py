"""Actual shared owner/journals: leverage replay equals the virtual session."""

from dataclasses import replace
from decimal import Decimal as D
from threading import Event

import pytest

from omega.domain.errors import ProtocolError, TradingBlocked, ValidationError
from omega.domain.margin import MarginBracket, MarginBracketSnapshot
from omega.domain.models import Candle, Instrument, Market
from omega.domain.trading import close_position
from omega.research.artifacts import Cancelled
from omega.research.datasets import MarketRow
from omega.research.margin_simulation import MarginSimulationSpec, _document, simulate_margin
from omega.runtime.margin_session import MarginTradingSession
from omega.strategies.target_quantity.module import TargetQuantityModule

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")


def configuration():
    account = {
        "account_id": "replay-test",
        "balance": "1000",
        "positions": [
            {
                "instrument": BTC.key,
                "owner_id": "btc",
                "margin_mode": "cross",
                "leverage": 10,
                "budget_margin": "100",
            },
            {
                "instrument": ETH.key,
                "owner_id": "eth",
                "margin_mode": "isolated",
                "leverage": 10,
                "budget_margin": "100",
                "isolated_collateral": "100",
            },
        ],
    }
    strategies = [
        {
            "id": identity,
            "instrument": item.key,
            "module": "target_quantity",
            "configuration": {"quantity": quantity},
            "analysis": {},
            "constraints": {"leverage": "10", "can_short": True},
        }
        for item, identity, quantity in ((BTC, "btc", "1"), (ETH, "eth", "-1"))
    ]
    return account, strategies


def tables():
    return tuple(
        MarginBracketSnapshot(
            item,
            (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),),
            0,
            0,
            "explicit_replay_test",
            "a" * 64,
        )
        for item in (BTC, ETH)
    )


def rows(count=4):
    result = []
    for index in range(count):
        for item, direction in ((BTC, 1), (ETH, -1)):
            opened = D(100 + direction * max(index - 1, 0))
            closed = D(100 + direction * index)
            result.append(
                MarketRow(
                    item.key,
                    index * 60000,
                    opened,
                    max(opened, closed),
                    min(opened, closed),
                    closed,
                    D(10),
                )
            )
    return result


def replay(root, values=None, **changes):
    account, strategies = configuration()
    params = dict(
        root=root,
        account=account,
        strategies=strategies,
        start_ms=0,
        brackets=tables(),
        session_factory=MarginTradingSession,
    )
    params.update(changes)
    return simulate_margin(rows() if values is None else values, **params)


def test_actual_cross_isolated_long_short_owner_parity_and_fee(tmp_path):
    result = replay(tmp_path / "replay")
    account, strategies = configuration()
    session = MarginTradingSession(
        tmp_path / "virtual", account, strategies, 0, create=True, fee_rate="0.001"
    )
    try:
        session.resume("human-start")
        for index in range(4):
            frame = tuple(
                Candle(
                    Instrument("binance", Market.USDM, row.instrument.split(":")[2]),
                    row.open_time_ms,
                    row.open_time_ms + 60000,
                    row.open,
                    row.high,
                    row.low,
                    row.close,
                    row.volume,
                )
                for row in rows()[index * 2 : index * 2 + 2]
            )
            session.run_once(frame, as_of_ms=(index + 1) * 60000, brackets=tables())
        final = session.execution.snapshot()
        assert result["initial_wallet"] == "1000" and result["events"] == []
        assert D(result["wallet"]) == D("999.800")
        assert result["account"]["cross_wallet"] == str(final.account.cross_wallet)
        assert result["account"]["positions"][1]["isolated_wallet"] == str(
            final.account.positions[1].isolated_wallet
        )
        assert result["positions"] == {BTC.key: "1", ETH.key: "-1"}
        assert len(result["fills"]) == len(final.account.ledger) == 2
        assert {item["available_at_ms"] for item in result["fills"]} == {120000}
        assert result["pending_orders"] == 0
        assert all(item["timestamp_ms"] > 0 for item in result["equity"])
        assert result["equity"][0] == {"timestamp_ms": 60000, "wallet": "1000", "equity": "1000"}
        assert result["account"] == _document(final.account)
        assert sum(D(item["fee"]) for item in result["fills"]) == D(".200")
        assert D(result["equity"][-1]["equity"]) == D("1005.800")
    finally:
        session.close()


def test_cancel_and_explicit_resume_reconstruct_full_curve_without_duplicate_fills(tmp_path):
    full = replay(tmp_path / "full")
    cancelled = Event()

    def progress(value):
        if value["completed_frames"] == 2:
            cancelled.set()

    with pytest.raises(Cancelled):
        replay(tmp_path / "resume", cancel=cancelled, progress=progress)
    restored = replay(tmp_path / "resume", resume=True)
    for key in ("account", "fills", "equity", "positions", "pending", "wallet", "cursor_ms"):
        assert restored[key] == full[key], key
    completed_again = replay(tmp_path / "resume", resume=True)
    assert completed_again["account"] == restored["account"]
    assert completed_again["equity"] == full["equity"]


def test_explicit_resume_after_start_ack_before_first_frame_releases_restart_pause(tmp_path):
    full = replay(tmp_path / "full")
    cancelled = Event()

    def factory(*args, **kwargs):
        session = MarginTradingSession(*args, **kwargs)
        original = session.resume

        def start_then_cancel(request_id):
            result = original(request_id)
            cancelled.set()
            return result

        session.resume = start_then_cancel
        return session

    with pytest.raises(Cancelled):
        replay(tmp_path / "resume", cancel=cancelled, session_factory=factory)
    restored = replay(tmp_path / "resume", resume=True)
    for key in ("account", "fills", "equity", "positions", "pending", "wallet", "cursor_ms"):
        assert restored[key] == full[key], key


def test_last_minute_pending_is_not_filled_by_invented_final_bar(tmp_path):
    result = replay(tmp_path, rows(1))
    assert result["fills"] == [] and result["wallet"] == "1000"
    assert result["pending_orders"] == 2
    assert result["positions"] == {BTC.key: "0", ETH.key: "0"}


@pytest.mark.parametrize("change", ["row", "configuration", "fee", "bracket"])
def test_resume_rejects_changed_lineage_before_opening_financial_owner(tmp_path, change):
    replay(tmp_path)
    arguments = {}
    data = rows()
    if change == "row":
        data[-1] = replace(data[-1], volume=D(11))
    elif change == "configuration":
        account, _ = configuration()
        account["balance"] = "1001"
        arguments["account"] = account
    elif change == "fee":
        arguments["spec"] = MarginSimulationSpec(D(".0001"))
    else:
        arguments["brackets"] = tuple(replace(item, checksum="b" * 64) for item in tables())
    calls = []
    arguments["session_factory"] = lambda *args, **kwargs: calls.append((args, kwargs))
    with pytest.raises(ProtocolError, match="changed"):
        replay(tmp_path, data, resume=True, **arguments)
    assert not calls


@pytest.mark.parametrize(
    "kind", ["gap", "missing", "duplicate", "future_reference", "stale", "far_gap"]
)
def test_invalid_input_preflight_does_not_create_namespace(tmp_path, kind):
    values, references = rows(), tables()
    start_ms = 0
    if kind == "gap":
        values = [item for item in values if item.open_time_ms != 60000]
    elif kind == "missing":
        values.pop()
    elif kind == "duplicate":
        values.append(values[0])
    elif kind == "future_reference":
        references = tuple(
            replace(item, observed_at_ms=60001, available_at_ms=60001) for item in references
        )
    elif kind == "stale":
        values = [replace(item, open_time_ms=item.open_time_ms + 86400000) for item in values]
        start_ms = 86400000
    else:
        values[-2:] = [replace(item, open_time_ms=253402300680000) for item in values[-2:]]
    with pytest.raises(ValidationError):
        replay(tmp_path / "untouched", values, brackets=references, start_ms=start_ms)
    assert not (tmp_path / "untouched").exists()


@pytest.mark.parametrize(
    "options",
    [
        {"funding": []},
        {"latency_ms": 0},
        {"path": "random"},
        {"slippage_rate": "0"},
        {"backend": "gpu"},
        {"fee_rate": "NaN"},
    ],
)
def test_unsupported_assumptions_are_explicit(options):
    with pytest.raises((ValidationError, ValueError)):
        MarginSimulationSpec.parse(options)


def test_lineage_is_exclusive_and_same_job_cannot_be_silently_reinitialized(tmp_path):
    replay(tmp_path)
    before = {path: path.read_bytes() for path in tmp_path.rglob("*.json")}
    with pytest.raises(FileExistsError):
        replay(tmp_path)
    assert before == {path: path.read_bytes() for path in tmp_path.rglob("*.json")}


def test_missing_replay_manifest_cannot_adopt_prior_financial_session(tmp_path):
    replay(tmp_path)
    (tmp_path / "margin-replay.json").unlink()
    original = {path: path.read_bytes() for path in tmp_path.rglob("*.json")}
    changed = [
        replace(row, open=row.open * 2, high=row.high * 2, low=row.low * 2, close=row.close * 2)
        for row in rows()
    ]
    with pytest.raises((ProtocolError, ValidationError, FileExistsError)):
        replay(tmp_path, changed)
    assert original == {path: path.read_bytes() for path in tmp_path.rglob("*.json")}


def test_realized_pnl_and_fees_are_applied_once_in_cross_and_isolated_wallets(tmp_path):
    class ClosingModule(TargetQuantityModule):
        def evaluate(self, context):
            if context.observed_at_ms >= 180000:
                return close_position(context, "explicit_test_close")
            return super().evaluate(context)

    class Registry:
        def create(self, identity, instrument, configuration):
            assert identity == "target_quantity"
            return ClosingModule(instrument, configuration)

    def factory(*args, **kwargs):
        return MarginTradingSession(*args, registry=Registry(), **kwargs)

    result = replay(tmp_path, session_factory=factory)
    assert result["positions"] == {BTC.key: "0", ETH.key: "0"}
    assert len(result["fills"]) == 4
    assert sum(D(item["realized_pnl"]) for item in result["fills"]) == D(4)
    assert sum(D(item["fee"]) for item in result["fills"]) == D(".4")
    assert D(result["wallet"]) == D("1003.6")
    assert D(result["account"]["cross_wallet"]) == D("901.798")
    assert D(result["account"]["positions"][1]["isolated_wallet"]) == D("101.802")
    again = replay(tmp_path, resume=True, session_factory=factory)
    assert again["account"] == result["account"]
    assert again["equity"] == result["equity"]


def test_updated_brackets_are_selected_at_their_original_availability(tmp_path):
    first = tables()
    later = tuple(
        replace(item, observed_at_ms=120000, available_at_ms=120000, checksum="b" * 64)
        for item in first
    )
    seen = []

    def factory(*args, **kwargs):
        session = MarginTradingSession(*args, **kwargs)
        original = session.run_once

        def run(candles, **options):
            seen.append(tuple(item.checksum for item in options["brackets"]))
            return original(candles, **options)

        session.run_once = run
        return session

    account, strategies = configuration()
    for item in strategies:
        item["configuration"]["quantity"] = "0"
    replay(
        tmp_path,
        account=account,
        strategies=strategies,
        brackets=later + first,
        session_factory=factory,
    )
    assert seen == [("a" * 64,) * 2] + [("b" * 64,) * 2] * 3


def test_unknown_financial_state_cannot_be_mapped_to_complete(tmp_path):
    sessions = []

    def factory(*args, **kwargs):
        session = MarginTradingSession(*args, **kwargs)
        original = session.run_once

        def run(*frame, **options):
            view = original(*frame, **options)
            session.execution._state = "unknown"  # Explicit outcome-loss fault injection.
            return view

        session.run_once = run
        sessions.append(session)
        return session

    with pytest.raises(TradingBlocked, match="not confirmed"):
        replay(tmp_path, session_factory=factory)
    assert sessions[0]._closed
    assert list(tmp_path.glob("state/margin-sessions/*.json"))


def test_failed_optional_progress_callback_retains_restartable_financial_files(tmp_path):
    def broken(_):
        raise RuntimeError("explicit report failure")

    with pytest.raises(RuntimeError, match="report failure"):
        replay(tmp_path, progress=broken)
    restored = replay(tmp_path, resume=True)
    assert len(restored["fills"]) == 2
    assert restored["pending_orders"] == 0
