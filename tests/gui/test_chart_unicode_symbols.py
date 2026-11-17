"""The GUI uses exact validated market identities, including Binance CJK symbols."""

import importlib.util
import os
from copy import deepcopy

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")

# Same public exchangeInfo identities as test_binance_symbol_identity.py;
# preserved source SHA256 e89230b75fe8050a978f4a459e903f5330354e702d1f20467845b7a702f79b18.
PUBLIC_UNICODE_SYMBOLS = [
    "\u5e01\u5b89\u4eba\u751fUSDT",
    "\u6211\u8e0f\u9a6c\u6765\u4e86USDT",
    "\u9f99\u867eUSDT",
    "\u725b\u6765USDT",
    "\u54c8\u57fa\u7c73USDT",
]


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["unicode-symbol-gui-tests"])


def client():
    from PySide6.QtCore import QObject, Signal

    class Client(QObject):
        candlesReceived = Signal(str, dict)
        connectionChanged = Signal(bool)
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls = []

        def request(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    return Client()


@pytest.mark.parametrize(
    "symbol", PUBLIC_UNICODE_SYMBOLS + ["BTCUSDT", "1000PEPEUSDT", "A" * 36 + "USDT"]
)
def test_chart_commit_saves_and_restores_exact_symbol(app, tmp_path, symbol):
    from urllib.parse import quote

    from omega.gui.charts_page import ChartsPage

    page = ChartsPage(client(), root=tmp_path)
    pane = page.panes[0]
    pane.symbol.setCurrentText(symbol)
    pane._commit_symbol()
    expected = "binance:usdm:" + symbol
    assert pane.selection.text() == expected
    assert pane.symbol.lineEdit().maxLength() == 40
    assert any(
        quote(expected, safe="") in args[1] for args, _ in page.client.calls if len(args) > 1
    )
    page.save_workspace()
    saved = page.store.load()
    assert saved["panes"][0]["instrument"] == expected
    restored = ChartsPage(client(), root=tmp_path)
    assert restored.panes[0].selection.text() == expected
    assert restored.panes[0].symbol.currentText() == symbol
    page.close()
    restored.close()
    page.deleteLater()
    restored.deleteLater()
    app.processEvents()


@pytest.mark.parametrize(
    "symbol",
    [
        "币/USDT",
        "币\\USDT",
        "币:USDT",
        "币\u202eUSDT",
        "币\u200bUSDT",
        "币\nUSDT",
        "ＢＴＣUSDT",
        "币A\u0301USDT",
        "币 USDT",
    ],
)
def test_chart_rejects_unsafe_symbols_without_changing_selection(app, symbol):
    from omega.gui.charts_page import ChartsPage

    page = ChartsPage(client())
    pane = page.panes[0]
    old = pane.selection.text()
    pane.symbol.setCurrentText(symbol)
    pane._commit_symbol()
    assert pane.selection.text() == old
    assert "Invalid" in pane.detail.text()
    page.close()
    page.deleteLater()
    app.processEvents()


def workspace(instrument):
    from omega.gui.chart_profiles import default_profile

    return {
        "schema_version": 1,
        "profiles": {},
        "panes": [{"instrument": instrument, "timeframe": 1, "profile": default_profile()}],
        "layout": "grid",
        "linked": True,
    }


@pytest.mark.parametrize(
    "instrument", ["binance:usdm:币安人生USDT", "binance:spot:测试USDT", "binance:spot:BTCUSDT"]
)
def test_workspace_uses_same_symbol_policy_without_identity_rewrite(instrument):
    from omega.gui.chart_profiles import validate_workspace

    document = workspace(instrument)
    before = deepcopy(document)
    assert validate_workspace(document) == before
    assert document == before


@pytest.mark.parametrize(
    "instrument",
    [
        "other:usdm:币安人生USDT",
        "binance:cash_equity:BTCUSDT",
        "binance:usdm:币\u200dUSDT",
        "binance:spot:ＢＴＣUSDT",
        "binance:usdm:币:USDT",
        None,
    ],
)
def test_saved_workspace_rejects_unsafe_or_other_market_identity(instrument):
    from omega.gui.chart_profiles import validate_workspace

    with pytest.raises(ValueError):
        validate_workspace(workspace(instrument))


def auxiliary_job(instrument):
    return {
        "id": "a" * 32,
        "kind": "trade_flow",
        "parameters": {"instrument": instrument},
        "execution_id": "b" * 32,
        "generation": 0,
        "state": "completed",
        "cancel_requested": False,
        "worker_started": True,
        "destination_id": "c" * 64,
        "archive_id": None,
        "result": {"complete": True},
        "error": None,
    }


@pytest.mark.parametrize("symbol", PUBLIC_UNICODE_SYMBOLS)
def test_auxiliary_unicode_job_can_be_selected_and_displayed(app, symbol):
    from omega.gui.auxiliary_panel import AuxiliaryPanel, _row

    identity = "binance:usdm:" + symbol
    row = auxiliary_job(identity)
    assert _row(row) is row
    panel = AuxiliaryPanel(client())
    panel.update_state({"core": {"epoch": "a", "capabilities": {"auxiliary_jobs": True}}})
    panel.set_instrument(identity)
    panel.setChecked(True)
    tag = panel.client.calls[-1][1]["tag"]
    panel.client.resultReceived.emit(tag, {"schema_version": 1, "core_epoch": "a", "jobs": [row]})
    assert panel.table.rowCount() == 1
    panel.table.selectRow(0)
    assert panel._selected()["parameters"]["instrument"] == identity
    assert panel.results.rowCount() == 1
    assert panel.submit_button.isEnabled()
    panel.timer.stop()
    panel.close()
    panel.deleteLater()
    app.processEvents()


@pytest.mark.parametrize(
    "instrument",
    [
        "binance:usdm:币\u202eUSDT",
        "binance:spot:币/USDT",
        "binance:spot:ＢＴＣUSDT",
        "other:usdm:BTCUSDT",
        "binance:usdm:A",
    ],
)
def test_auxiliary_rows_reject_unsafe_identity(instrument):
    from omega.gui.auxiliary_panel import _row

    with pytest.raises(ValueError):
        _row(auxiliary_job(instrument))


def test_ascii_lowercase_editor_keeps_existing_uppercase_commit_behavior(app):
    from omega.gui.charts_page import ChartsPage

    page = ChartsPage(client())
    pane = page.panes[0]
    pane.symbol.setCurrentText("ethusdt")
    pane._commit_symbol()
    assert pane.selection.text() == "binance:usdm:ETHUSDT"
    page.close()
    page.deleteLater()
    app.processEvents()
