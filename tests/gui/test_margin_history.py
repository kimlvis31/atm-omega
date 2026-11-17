"""Typed financial facts are projected without UI financial reconstruction."""

import importlib.util
from copy import deepcopy
from decimal import Decimal as D

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional PySide6 is not installed"
)


@pytest.fixture(scope="module")
def app():
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["margin-history-tests"])


@pytest.fixture
def view(app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.margin_history import MarginHistoryWidget

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        connectionChanged = Signal(bool)

        def __init__(self):
            super().__init__()
            self.attached, self.calls = True, []

        def request(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    widget = MarginHistoryWidget(Client())
    yield widget
    widget.close()
    widget.deleteLater()
    app.processEvents()


def packet():
    from omega.domain.margin import CollateralTransfer, FillEvent, apply_fill, apply_transfer
    from omega.domain.models import Instrument, Market
    from omega.runtime.margin_console import initial_account
    from omega.runtime.margin_store import account_document

    btc, eth = (Instrument("binance", Market.USDM, symbol) for symbol in ("BTCUSDT", "ETHUSDT"))
    account = initial_account(
        {
            "account_id": "a",
            "balance": "1000",
            "positions": [
                {
                    "instrument": btc.key,
                    "owner_id": "one",
                    "margin_mode": "cross",
                    "leverage": 10,
                    "budget_margin": "100",
                },
                {
                    "instrument": eth.key,
                    "owner_id": "two",
                    "margin_mode": "isolated",
                    "leverage": 10,
                    "budget_margin": "100",
                    "isolated_collateral": "100",
                },
            ],
        }
    )
    account = apply_transfer(
        account, CollateralTransfer("transfer", eth, "two", D(10), 0, 1)
    ).account
    account = apply_fill(
        account,
        FillEvent(
            "fill-one", "order-one", btc, "one", D(1), D(100), D(".1"), "USDT", 60000, 120000
        ),
    ).account
    account = apply_fill(
        account,
        FillEvent(
            "fill-two", "order-two", btc, "one", D(-1), D(104), D(".104"), "USDT", 180000, 240000
        ),
    ).account
    return {
        "scope": "virtual_margin_v2_ohlcv_proxy",
        "core_epoch": "first",
        "accounts": [
            {
                "account_id": "a",
                "state": "ready",
                "account": account_document(account, ledger_limit=128),
            }
        ],
    }


def state(epoch="first"):
    return {"core": {"epoch": epoch, "capabilities": {"margin_trading": True}}}


def accept(view, data=None):
    view.update_state(state())
    view.client.resultReceived.emit(view.pending[0], data or packet())


def test_real_ledger_fill_transfer_sequence_fees_and_pnl_are_preserved(view):
    data = packet()
    original = deepcopy(data)
    accept(view, data)
    assert view.table.rowCount() == 3
    assert view.table.item(0, 0).text() == "3"
    assert view.table.item(0, 5).text() == "SELL"
    assert view.table.item(0, 6).text() == "-1"
    assert view.table.item(0, 8).text() == "0.104 USDT"
    assert view.table.item(0, 9).text() == "4 USDT"
    assert view.table.item(2, 5).text() == "TRANSFER"
    assert view.table.item(2, 6).text() == "10"
    assert "order-two" in view.table.item(0, 0).toolTip()
    assert data == original
    assert all(call[0][0] == "GET" for call in view.client.calls)


def test_strategy_selection_and_search_never_mix_accounts_or_transfer_rows(view):
    accept(view)
    view.strategy.setCurrentIndex(view.strategy.findData("one"))
    assert view.table.rowCount() == 2
    view.search.setText("fill-two")
    assert view.table.rowCount() == 1
    view.strategy.setCurrentIndex(view.strategy.findData("two"))
    assert view.table.rowCount() == 0
    view.search.clear()
    assert view.table.rowCount() == 1 and view.table.item(0, 5).text() == "TRANSFER"


def test_stale_core_response_and_disconnection_retain_labelled_prior_rows(view):
    accept(view)
    view.refresh()
    tag = view.pending[0]
    view.update_state(state("second"))
    new_tag = view.pending[0]
    view.client.resultReceived.emit(tag, packet())
    assert view.pending[0] == new_tag and view.table.rowCount() == 3
    view.client.resultReceived.emit(new_tag, packet())  # old epoch on new request
    assert "Stale" in view.status.text() or "이전" in view.status.text()
    view.client.attached = False
    view.client.connectionChanged.emit(False)
    assert view.table.rowCount() == 3 and not view.refresh_button.isEnabled()
    assert "이전" in view.status.text()


def test_unknown_owner_and_truncated_history_are_explicit(view):
    data = packet()
    row = data["accounts"][0]
    row["state"] = "unknown"
    envelope = row["account"]
    envelope["account"]["ledger"] = envelope["account"]["ledger"][-1:]
    envelope["summary"].update(ledger_limit=1, ledger_truncated=True)
    accept(view, data)
    assert view.table.rowCount() == 1
    assert "unknown" in view.context.text() and "생략" in view.context.text()
    assert "3" in view.context.text()


@pytest.mark.parametrize("corruption", ["sequence", "identity", "scope", "nan", "oversized"])
def test_malformed_ledger_does_not_replace_confirmed_rows(view, corruption):
    accept(view)
    data = packet()
    row = data["accounts"][0]["account"]
    if corruption == "sequence":
        row["account"]["ledger"][1]["sequence"] = 1
    elif corruption == "identity":
        row["account"]["ledger"][0]["fact"]["owner_id"] = "other"
    elif corruption == "scope":
        data["scope"] = "live"
    elif corruption == "nan":
        row["account"]["ledger"][0]["fee"] = "NaN"
    else:
        row["account"]["ledger"] *= 50
    view.refresh()
    view.client.resultReceived.emit(view.pending[0], data)
    assert view.table.rowCount() == 3
    assert (
        "Invalid" in view.status.text()
        or "mismatch" in view.status.text()
        or "Nonfinite" in view.status.text()
    )


def test_unavailable_account_is_not_relabelled_flat(view):
    data = {
        "scope": "virtual_margin_v2_ohlcv_proxy",
        "core_epoch": "first",
        "accounts": [
            {"account_id": "bad", "state": "unavailable", "error": "StorageError", "account": None}
        ],
    }
    accept(view, data)
    assert view.table.rowCount() == 0 and "unavailable" in view.context.text()
    assert "—" in view.context.text()


def test_missing_recorded_pnl_stays_unavailable(view):
    data = packet()
    del data["accounts"][0]["account"]["account"]["ledger"][-1]["realized_pnl"]
    accept(view, data)
    assert view.table.item(0, 9).text() == "— USDT"


def test_equal_account_ids_in_paper_and_margin_are_independent_tabs(view):
    from omega.gui.history_page import HistoryPage

    page = HistoryPage(view.client)
    try:
        snapshot = state() | {
            "accounts": [{"account_id": "a", "quote_asset": "BTC", "cash": "4"}],
            "strategies": [
                {"account_id": "a", "instance": "paper", "instrument": "binance:spot:ETHBTC"}
            ],
        }
        page.update_state(snapshot)
        page.client.resultReceived.emit(page.margin_history.pending[0], packet())
        assert page._accounts["a"]["quote_asset"] == "BTC"
        assert page.margin_history.accounts["a"]["asset"] == "USDT"
        assert page._fills == [] and page.margin_history.table.rowCount() == 3
        assert page.history_tabs.count() == 3
        assert page.history_tabs.widget(2) is page.ledger_history
    finally:
        page.close()
        page.deleteLater()
