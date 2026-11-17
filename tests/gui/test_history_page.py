"""The history view reports observed fills without inventing account PnL."""

import importlib.util
from decimal import Decimal

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None,
    reason="optional Qt dependency is not installed",
)


@pytest.fixture(scope="module")
def qt_app():
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    from omega.gui.presentation import configure_fonts

    app = QApplication.instance() or QApplication(["history-view-tests"])
    configure_fonts(app)
    return app


@pytest.fixture
def page(qt_app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.history_page import HistoryPage

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        connectionChanged = Signal(bool)

        def __init__(self):
            super().__init__()
            self.attached, self.calls = True, []

        def request(self, *args, **kwargs):
            self.calls.append((args, kwargs))

        def connected(self, value):
            self.attached = value
            self.connectionChanged.emit(value)

    instance = HistoryPage(Client())
    instance.set_presentation("light", "en")
    instance.resize(1200, 800)
    instance.show()
    qt_app.processEvents()
    yield instance
    instance.close()
    instance.deleteLater()
    qt_app.processEvents()


def snapshot(revision=1, epoch="boot-one"):
    return {
        "core": {"epoch": epoch},
        "accounts": [
            {"account_id": "first", "market": "usdm", "cash": "10000", "quote_asset": "USDT"},
            {"account_id": "second", "market": "spot", "cash": "4", "quote_asset": "BTC"},
        ],
        "strategies": [
            {
                "instance": "tef-a",
                "account_id": "first",
                "instrument": "binance:usdm:BTCUSDT",
                "revision": revision,
                "fill_count": 2,
            },
            {
                "instance": "other-b",
                "account_id": "second",
                "instrument": "binance:spot:ETHBTC",
                "revision": revision,
                "fill_count": 1,
            },
        ],
    }


def detail(*, instance="tef-a", account="first", fills=None):
    return {
        "instance": instance,
        "account_id": account,
        "instrument": "binance:usdm:BTCUSDT" if account == "first" else "binance:spot:ETHBTC",
        "fills": fills
        if fills is not None
        else [
            {
                "timestamp_ms": 60_000,
                "side": "BUY",
                "quantity": "0.05",
                "price": "60000",
                "fee": "0.10001",
                "reason": "entry",
            },
            {
                "timestamp_ms": 120_000,
                "side": "SELL",
                "quantity": "0.02",
                "price": "61000",
                "fee": "0.20002",
                "reason": "reduce",
            },
        ],
    }


def receive(page, value=None):
    tag = page.client.calls[-1][1]["tag"]
    page.client.resultReceived.emit(tag, value or detail())
    return tag


def test_account_strategy_selection_uses_only_detail_api_and_does_not_invent_pnl(page):
    page.update_state(snapshot())
    assert len(page.client.calls) == 1
    assert page.client.calls[0][0] == ("GET", "/api/strategies/tef-a")
    receive(page)
    assert page.fills_table.rowCount() == 2
    assert page.fills_table.item(0, 0).text() == "1970/01/01 00:02:00"
    assert page.fills_table.item(0, 1).text() == "binance:usdm:BTCUSDT"
    assert page.fills_table.item(0, 7).text() == "—"
    assert "0.30003 USDT" in page.fee_summary.text()
    assert "Not provided" in page.pnl_summary.text()
    assert "10000 USDT" in page.account_info.text()
    assert "4 BTC" not in page.account_info.text()
    assert "1,000" in page.limit_note.text()
    assert "minute-close" in page.limit_note.text()


def test_switch_account_clears_old_rows_and_ignores_late_response(page):
    page.update_state(snapshot())
    old = receive(page)
    page.select_account("second")
    assert page.fills_table.rowCount() == 0
    assert "other-b" in page.selection_label.text()
    assert page.client.calls[-1][0] == ("GET", "/api/strategies/other-b")
    page.client.resultReceived.emit(old, detail())
    assert page.fills_table.rowCount() == 0
    receive(
        page,
        detail(
            instance="other-b",
            account="second",
            fills=[
                {
                    "timestamp_ms": 0,
                    "side": "BUY",
                    "quantity": "1",
                    "price": "0.03",
                    "fee": "0.00001",
                },
            ],
        ),
    )
    assert "0.00001 BTC" in page.fee_summary.text()
    assert "USDT" not in page.fee_summary.text()


def test_search_and_side_filters_do_not_change_loaded_summary(page):
    page.update_state(snapshot())
    receive(page)
    page.search.setText("ENTRY")
    assert page.fills_table.rowCount() == 1
    assert "1 / 2" in page.count_label.text()
    assert "Loaded-record fees" in page.fee_summary.text()
    page.side.setCurrentIndex(2)
    assert page.fills_table.rowCount() == 0
    page.search.clear()
    assert page.fills_table.rowCount() == 1
    assert page.fills_table.item(0, 2).text() == "SELL"


def test_failed_or_disconnected_records_are_marked_stale_and_reconnect_refreshes(page):
    page.update_state(snapshot())
    receive(page)
    page.refresh()
    page.client.failed.emit(page._pending_tag, "service unavailable")
    assert page.fills_table.rowCount() == 2
    assert "Request failed" in page.status.text()
    assert "service unavailable" in page.status.text()
    count = len(page.client.calls)
    page.client.connected(False)
    assert "Disconnected" in page.status.text()
    assert not page.refresh_button.isEnabled()
    page.client.connected(True)
    assert len(page.client.calls) == count + 1
    assert "Loading" in page.status.text()
    receive(page)
    assert "Observed" in page.status.text()


def test_pending_revision_change_and_restart_force_new_observation(page):
    page.update_state(snapshot())
    page.update_state(snapshot(revision=2))
    assert len(page.client.calls) == 1
    receive(page)
    assert len(page.client.calls) == 2
    receive(page)
    page.update_state(snapshot(revision=2))
    assert len(page.client.calls) == 2
    page.update_state(snapshot(revision=2, epoch="boot-two"))
    assert len(page.client.calls) == 3


def test_mismatched_response_does_not_replace_observed_fills(page):
    page.update_state(snapshot())
    receive(page)
    page.refresh()
    receive(page, detail(account="second"))
    assert page.fills_table.rowCount() == 2
    assert "mismatch" in page.status.text()


def test_bounded_recent_records_and_fee_assets_are_kept_separate(page):
    page.update_state(snapshot())
    rows = [
        {
            "timestamp_ms": index * 60_000,
            "side": "BUY",
            "quantity": "1",
            "price": "1",
            "fee": "0.1",
            "fee_asset": "BNB" if index % 2 else "USDT",
        }
        for index in range(1002)
    ]
    receive(page, detail(fills=rows))
    assert len(page._fills) == page.fills_table.rowCount() == 1000
    assert page._fills[0]["timestamp_ms"] == 120_000
    assert "50.0 BNB" in page.fee_summary.text()
    assert "50.0 USDT" in page.fee_summary.text()


def test_partial_nonfinite_and_mixed_currency_pnl_is_never_fabricated(page):
    from omega.gui.history_page import recorded_decimal, recorded_time

    assert recorded_decimal("0.100000000000000000001") == Decimal("0.100000000000000000001")
    assert recorded_decimal("NaN") is None
    assert recorded_decimal(True) is None
    assert recorded_time(-1) == recorded_time(True) == "—"
    page.update_state(snapshot())
    receive(
        page,
        detail(
            fills=[
                {
                    "timestamp_ms": 0,
                    "quantity": "NaN",
                    "price": "Infinity",
                    "pnl": "1",
                    "pnl_asset": "USDT",
                },
                {"timestamp_ms": 60_000, "pnl": "0.2", "pnl_asset": "BTC"},
            ]
        ),
    )
    assert "0.2 BTC" in page.pnl_summary.text()
    assert "1 USDT" in page.pnl_summary.text()
    assert "1.2" not in page.pnl_summary.text()
    assert page.fills_table.item(1, 3).text() == "—"
    page.refresh()
    receive(page, detail(fills=[{"pnl": "0"}, {}]))
    assert "Partial records only" in page.pnl_summary.text()
    assert "0 USDT" not in page.pnl_summary.text()


def test_empty_accounts_remove_stale_selection_and_disable_query(page):
    page.update_state(snapshot())
    receive(page)
    page.update_state({"core": {"epoch": "boot-one"}, "accounts": [], "strategies": []})
    assert page.fills_table.rowCount() == 0
    assert not page._selected_account and not page._selected_strategy
    assert not page.refresh_button.isEnabled()


def test_shell_embedding_hides_only_redundant_heading(page):
    assert not page.title.isHidden()
    page.set_shell_embedded(True)
    assert page.title.isHidden()
    assert not page.source_note.isHidden()
    assert not page.refresh_button.isHidden()
    assert not page.accounts_table.isHidden()
    page.set_shell_embedded(False)
    assert not page.title.isHidden()
