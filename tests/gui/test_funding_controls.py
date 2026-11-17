"""Explicit funding requests, unchanged legacy drafts, and signed financial views."""

import json
import os
from copy import deepcopy
from decimal import Decimal as D

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QDateTime, QObject, QPoint, QTimeZone, Signal
from PySide6.QtWidgets import QApplication, QFileDialog

from omega.application.margin_funding import FundingPoint, prepare_funding_batch
from omega.domain.margin import FillEvent, apply_fill, apply_funding
from omega.domain.models import Instrument, Market
from omega.gui.funding_editor import PREVIEW_ROWS, FundingEditor
from omega.gui.margin_history import MarginHistoryWidget, ledger_accounts
from omega.gui.margin_page import MarginPage
from omega.gui.margin_replay_controls import MarginReplayControls
from omega.gui.pages import ResearchPage
from omega.gui.presentation import configure_fonts, stylesheet
from omega.gui.simulation_results import SimulationResultsPage
from omega.research.result_summary import summarize_simulation
from omega.runtime.margin_console import initial_account
from omega.runtime.margin_store import account_document

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
SCOPE = "minute_boundary_scenario_rate_v1"


def schedule(count=1):
    return {
        "schema_version": 1,
        "model": "linear_usdt_scenario_rate_v1",
        "points": [
            {
                "instrument": BTC.key,
                "event_time_ms": i * 60000,
                "source_available_at_ms": i * 60000 + 1000,
                "reference_time_ms": i * 60000,
                "rate": ".0100",
                "reference_price": "+100.00",
            }
            for i in range(count)
        ],
    }


class Client(QObject):
    resultReceived = Signal(str, dict)
    failed = Signal(str, str)
    connectionChanged = Signal(bool)
    stateReceived = Signal(dict)
    modulesReceived = Signal(dict)
    attached = True
    root = None

    def __init__(self):
        super().__init__()
        self.calls, self.jobs = [], []
        self.state = {
            "core": {
                "epoch": "core",
                "capabilities": {
                    "margin_trading": True,
                    "margin_simulation": True,
                    "margin_funding": True,
                    "margin_funding_scope": SCOPE,
                },
            }
        }

    def request(self, method, path, payload=None, *, tag):
        self.calls.append((method, path, deepcopy(payload), tag))

    def submit_job(self, kind, parameters):
        self.jobs.append((kind, deepcopy(parameters)))
        return "a" * 32


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    return QApplication.instance() or QApplication(["funding-controls"])


@pytest.fixture
def page(app):
    page = MarginPage(Client())
    page.timer.stop()
    page.update_state(page.client.state)
    page.apply_snapshot(
        {
            "core_epoch": "core",
            "state": "ready",
            "accounts": [],
            "operations": [],
            "capabilities": {"funding": True, "funding_scope": SCOPE},
        }
    )
    for control in (page.start, page.bracket_time):
        control.setDateTime(QDateTime.fromMSecsSinceEpoch(0, QTimeZone(b"UTC")))
    page.module.setCurrentIndex(page.module.findData("target_quantity"))
    page.save_strategy()
    page.manual_assumptions.setChecked(True)
    yield page
    page.close()
    page.deleteLater()
    app.processEvents()


def test_optional_file_selection_preserves_source_decimal_strings_and_off_omits_key(
    page, tmp_path, monkeypatch
):
    before = page.parameters()
    assert "funding" not in before and not page.funding.isChecked()
    path = tmp_path / "scenario.json"
    raw_bytes = json.dumps(schedule(), indent=4).encode()
    path.write_bytes(raw_bytes)
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *args: (str(path), "JSON"))
    page.funding.select_file()
    assert page.parameters()["funding"] == schedule()
    assert page.parameters()["funding"]["points"][0]["rate"] == ".0100"
    assert path.read_bytes() == raw_bytes
    assert not page.client.calls and not page.client.jobs
    page.funding.setChecked(False)
    assert page.parameters() == before
    assert path.read_bytes() == raw_bytes


def test_changed_or_unreadable_source_blocks_submit_without_replacing_draft(
    page, tmp_path, monkeypatch
):
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(schedule()), encoding="utf-8")
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *args: (str(path), "JSON"))
    page.funding.select_file()
    old = deepcopy(page.funding.raw)
    path.write_text("{}", encoding="utf-8")
    page.configure()
    assert not any(row[0] == "POST" for row in page.client.calls)
    assert "changed" in page.status.text()
    assert page.funding.raw == old
    path.unlink()
    with pytest.raises(ValueError, match="readable"):
        page.parameters()


def test_invalid_or_oversize_candidate_keeps_original_file_and_entire_draft(
    page, tmp_path, monkeypatch
):
    page.funding.load_document(schedule())
    old = page.parameters()
    bad = tmp_path / "bad.json"
    bad.write_text(
        json.dumps({"schema_version": 1, "model": "actual_venue_payment", "points": []}),
        encoding="utf-8",
    )
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *args: (str(bad), "JSON"))
    page.funding.select_file()
    assert page.parameters() == old
    assert page.funding.status.text().startswith("새 파일을 불러오지 못해 이전 선택을 유지합니다: ")
    page.funding.set_language("en")
    page.funding.select_file()
    assert page.parameters() == old
    assert page.funding.status.text().startswith(
        "Could not load the new file; the previous selection is retained: "
    )
    page.funding.validate_candidate = lambda value: {"funding": value, "padding": "x" * 131072}
    with pytest.raises(ValueError, match="131072"):
        page.funding.load_document(schedule(2))
    assert page.parameters() == old


def test_resize_reflows_module_before_following_bracket_group_is_painted(page, app):
    configure_fonts(app)
    page.setStyleSheet(stylesheet("light"))
    page.funding.load_document(schedule())
    page.tabs.setCurrentIndex(1)
    content = page.editor_panel.parentWidget()
    bracket = page.bracket_cap.parentWidget()
    draft = page.parameters()
    page.show()
    app.processEvents()
    for width in (1360, 800, 1360, 800):
        page.resize(width, 950)
        app.processEvents()
        page.tabs.widget(1).ensureWidgetVisible(page.funding)
        app.processEvents()
        quantity = page.configuration_editor.controls[("quantity",)][0]
        quantity_bottom = quantity.mapTo(content, QPoint(0, quantity.height())).y()
        assert quantity_bottom < bracket.y()
        assert page.editor_panel.geometry().bottom() < bracket.y()
        assert page.parameters() == draft
    assert not any(row[0] == "POST" for row in page.client.calls)


def test_preview_count_is_bounded_without_truncating_transmitted_schedule(app):
    editor = FundingEditor()
    editor.set_supported(True)
    editor.load_document(schedule(PREVIEW_ROWS + 1))
    assert editor.preview.rowCount() == PREVIEW_ROWS
    assert f"200 / {PREVIEW_ROWS + 1}" in editor.summary.text()
    assert len(editor.document()["points"]) == PREVIEW_ROWS + 1
    assert not editor.path.isUndoAvailable()
    editor.close()


@pytest.mark.parametrize(
    "capabilities",
    [
        {},
        {"funding": 1, "funding_scope": SCOPE},
        {"funding": True, "funding_scope": "venue"},
        {"funding": "true", "funding_scope": SCOPE},
    ],
)
def test_margin_capability_requires_native_true_and_exact_scope(page, capabilities):
    page.apply_snapshot(
        {
            "core_epoch": "core",
            "state": "ready",
            "accounts": [],
            "operations": [],
            "capabilities": capabilities,
        }
    )
    assert not page.funding.supported
    with pytest.raises(ValueError, match="support"):
        page.funding.load_document(schedule())
    assert "funding" not in page.parameters()


def test_margin_account_scope_validation_does_not_silently_change_selected_instrument(page):
    before = page.parameters()
    value = schedule()
    value["points"][0]["instrument"] = "binance:usdm:ETHUSDT"
    with pytest.raises(ValueError, match="conflicts"):
        page.funding.load_document(value)
    assert page.parameters() == before


def test_replay_load_and_reset_preserve_schedule_and_require_new_manual_consent(app):
    form = MarginReplayControls()
    try:
        form.funding.set_supported(True)
        form.add_strategy()
        form.manual.setChecked(True)
        legacy = form.parameters()
        form.funding.load_document(schedule())
        saved = form.parameters()
        form.load_parameters(saved)
        assert form.funding.raw == schedule() and form.funding.isChecked()
        assert not form.manual.isChecked()
        form.manual.setChecked(True)
        assert form.parameters() == saved
        invalid = deepcopy(saved)
        invalid["account"]["balance"] = "50000"
        invalid["funding"]["model"] = "venue_payment"
        with pytest.raises(ValueError):
            form.load_parameters(invalid)
        assert form.parameters() == saved
        form.load_parameters(legacy)
        form.manual.setChecked(True)
        assert form.parameters() == legacy
        assert form.funding.raw is None and not form.funding.isChecked()
    finally:
        form.close()
        form.deleteLater()


@pytest.mark.parametrize(
    "capabilities",
    [
        {},
        {"margin_funding": 1, "margin_funding_scope": SCOPE},
        {"margin_funding": True, "margin_funding_scope": "venue"},
    ],
)
def test_research_mode_capability_invalidation_blocks_funded_request_without_implicit_job(
    app, capabilities
):
    client = Client()
    page = ResearchPage(client, ["simulate"])
    page.simulation_mode.setCurrentIndex(page.simulation_mode.findData("margin"))
    form = page.margin_replay
    form.add_strategy()
    form.manual.setChecked(True)
    form.funding.load_document(schedule())
    assert page.run_button.isEnabled()
    assert not client.jobs
    client.state["core"]["capabilities"] = {"margin_simulation": True, **capabilities}
    page.set_attached(True)
    assert not form.funding.supported and not page.run_button.isEnabled()
    assert form.funding.raw == schedule()
    assert form.funding.isChecked()
    assert page.simulation_mode.currentData() == "margin"
    form.funding.setChecked(False)
    assert page.run_button.isEnabled()
    client.attached = False
    page.set_attached(False)
    assert not form.funding.supported and not page.run_button.isEnabled()
    assert not client.jobs
    page.close()


def test_core_change_preserves_selected_file_checked_state_and_draft_until_explicit_off(
    page, tmp_path, monkeypatch
):
    path = tmp_path / "chosen.json"
    path.write_text(json.dumps(schedule()), encoding="utf-8")
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *args: (str(path), "JSON"))
    page.funding.select_file()
    saved_rows = deepcopy(page.rows)
    page.update_state({"core": {"epoch": "old-core", "capabilities": {"margin_trading": True}}})
    page.apply_snapshot(
        {
            "core_epoch": "old-core",
            "state": "ready",
            "accounts": [],
            "operations": [],
            "capabilities": {},
        }
    )
    assert page.funding.isChecked() and page.funding.source_path == str(path)
    assert page.funding.raw == schedule() and page.rows == saved_rows
    assert not page.funding.supported and not page.configure_button.isEnabled()
    page.configure()
    assert not any(row[0] == "POST" for row in page.client.calls)
    with pytest.raises(ValueError, match="supported"):
        page.parameters()
    page.funding.setChecked(False)
    assert "funding" not in page.parameters()
    assert page.funding.raw == schedule() and page.funding.source_path == str(path)


def test_scope_is_checked_again_after_strategy_draft_changes(page):
    page.funding.load_document(schedule())
    page.symbol.setText("ETHUSDT")
    page.save_strategy()
    with pytest.raises(ValueError, match="conflicts"):
        page.parameters()
    page.configure()
    assert not any(row[0] == "POST" for row in page.client.calls)
    assert page.funding.raw == schedule()


def ledger_packet():
    account = initial_account(
        {
            "account_id": "fund",
            "balance": "1000",
            "positions": [
                {
                    "instrument": BTC.key,
                    "owner_id": "ts",
                    "margin_mode": "cross",
                    "leverage": 1,
                    "budget_margin": "100",
                }
            ],
        }
    )
    account = apply_fill(
        account, FillEvent("entry", "order", BTC, "ts", D(1), D(100), D(0), "USDT", 0, 60000)
    ).account
    for timestamp, rate in ((60000, ".01"), (120000, "-.02"), (180000, "0")):
        point = FundingPoint(BTC, timestamp, timestamp, timestamp, D(rate), D(100))
        plan = prepare_funding_batch(account, (point,), as_of_ms=timestamp + 60000)
        account = apply_funding(account, plan.facts[0]).account
    return {
        "scope": "virtual_margin_v2_ohlcv_proxy",
        "core_epoch": "core",
        "accounts": [
            {
                "account_id": "fund",
                "state": "ready",
                "account": account_document(account, ledger_limit=128),
            }
        ],
    }


def test_funding_history_is_never_classified_as_transfer_and_sources_are_explicit(app):
    packet = ledger_packet()
    records = ledger_accounts(packet)["fund"]["ledger_rows"]
    assert [row["direction"] for row in records] == [
        "BUY",
        "FUNDING_DEBIT",
        "FUNDING_CREDIT",
        "FUNDING_ZERO",
    ]
    assert all(row["event_id"].startswith("funding:") for row in records[1:])
    client = Client()
    view = MarginHistoryWidget(client)
    try:
        view.update_state(client.state)
        view.client.resultReceived.emit(view.pending[0], packet)
        assert view.table.rowCount() == 4 and view.table.columnCount() == 14
        assert view.table.item(0, 12).text() == "시나리오 rate / Scenario rate"
        assert view.table.item(0, 13).text().startswith("funding:")
        assert "USDT" in view.table.item(0, 6).text()
        assert "실지급 아님" in view.context.text()
    finally:
        view.close()
        view.deleteLater()


def test_margin_operations_accept_actual_funding_codec_packet(page):
    packet = ledger_packet()
    packet.update(
        state="ready", operations=[], capabilities={"funding": True, "funding_scope": SCOPE}
    )
    document = packet["accounts"][0]["account"]
    assert document["schema_version"] == 2
    assert document["summary"]["ledger_total"] == 4
    assert D(document["account"]["ledger"][-1]["fact"]["amount"]) == 0

    page.apply_snapshot(packet)
    page.account_table.selectRow(0)
    assert page.selected_account()["account"] == document
    assert page.position_table.item(0, 3).text() == "1"
    assert "Ledger: 4" in page.account_summary.text()
    assert not any(row[0] == "POST" for row in page.client.calls)


@pytest.mark.parametrize("invalid", [True, 3, "2", "account_shape"])
def test_margin_operations_reject_invalid_funding_packet_without_losing_view(page, invalid):
    packet = ledger_packet()
    packet.update(
        state="ready", operations=[], capabilities={"funding": True, "funding_scope": SCOPE}
    )
    page.apply_snapshot(packet)
    page.account_table.selectRow(0)
    previous = deepcopy(page.accounts)
    malformed = deepcopy(packet)
    document = malformed["accounts"][0]["account"]
    if invalid == "account_shape":
        document["account"] = []
    else:
        document["schema_version"] = invalid
    with pytest.raises(ValueError, match="invalid account document"):
        page.apply_snapshot(malformed)
    assert page.accounts == previous
    assert page.selected_account()["account_id"] == "fund"
    assert page.position_table.item(0, 3).text() == "1"
    assert not any(row[0] == "POST" for row in page.client.calls)


@pytest.mark.parametrize("change", ["old_schema", "amount", "source"])
def test_malformed_funding_projection_is_rejected_not_converted_to_transfer(change):
    packet = ledger_packet()
    envelope = packet["accounts"][0]["account"]
    if change == "old_schema":
        envelope["schema_version"] = 1
    elif change == "amount":
        envelope["account"]["ledger"][1]["fact"]["amount"] = "999"
    else:
        envelope["account"]["ledger"][1]["fact"]["source_kind"] = "actual_venue_payment"
    with pytest.raises(ValueError):
        ledger_accounts(packet)


def test_result_view_displays_signed_net_separately_and_restores_legacy_columns(app):
    record = {
        "schema_version": 1,
        "state": "complete",
        "scope": "virtual_margin_v2_ohlcv_proxy",
        "quote_asset": "USDT",
        "initial_wallet": "100",
        "wallet": "98",
        "fills": [],
        "events": [
            {
                "kind": "funding_cashflow",
                "timestamp_ms": 60000,
                "amount": "-2",
                "settlement_asset": "USDT",
                "source_kind": "scenario_rate",
            }
        ],
        "equity": [{"timestamp_ms": 120000, "wallet": "98", "equity": "98"}],
        "funding": {"funding_net": "-2", "source_kind": "scenario_rate"},
    }
    page = SimulationResultsPage(Client())
    page.accept_result(
        {"path": "fixture.json", "sha256": "a" * 64, "summary": summarize_simulation(record)}
    )
    assert page.costs.columnCount() == 5
    assert page.costs.item(0, 2).text() == "0"
    assert page.costs.item(0, 3).text() == "-2"
    assert "Signed scenario" in page.costs.horizontalHeaderItem(3).text()
    assert "-2 USDT" in page.events.item(0, 3).text()
    legacy = deepcopy(record)
    del legacy["funding"]
    legacy["events"] = []
    legacy["wallet"] = "100"
    legacy["equity"][0].update(wallet="100", equity="100")
    page.accept_result(
        {"path": "legacy.json", "sha256": "b" * 64, "summary": summarize_simulation(legacy)}
    )
    assert page.costs.columnCount() == 4
    assert page.costs.horizontalHeaderItem(2).text() == "Funding cost"
    page.close()
