"""Structured scenario drafts and confirmed cost quotes, with no financial execution."""

import json
import os
from copy import deepcopy
from decimal import Decimal as D
from hashlib import sha256

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QDateTime, QObject, QPoint, QTimeZone, Signal
from PySide6.QtWidgets import QApplication

from omega.application.margin_cost import MarginCostPolicy
from omega.domain.margin import FillEvent
from omega.domain.models import Instrument, Market
from omega.gui.cost_editor import COST_SCOPE, CostEditor
from omega.gui.margin_page import MarginPage
from omega.gui.margin_replay_controls import MarginReplayControls
from omega.gui.pages import ResearchPage
from omega.gui.presentation import configure_fonts, stylesheet
from omega.gui.simulation_results import COST_QUOTE_PREVIEW_ROWS, SimulationResultsPage
from omega.research.margin_simulation import _document
from omega.research.result_summary import summarize_simulation

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def policy(**changes):
    return {
        "schema_version": 1,
        "model": "linear_usdt_ohlcv_cost_v1",
        "slippage_rate": "0.0100",
        "execution_price_bound_rate": "0.1000",
        "mark_drift_bound_rate": "0.1000",
        "entry_delay_ms": 1,
        "reduction_delay_ms": 0,
        "limit_price_policy": "adverse_then_cap_v1",
    } | changes


class Client(QObject):
    resultReceived = Signal(str, dict)
    failed = Signal(str, str)
    connectionChanged = Signal(bool)
    stateReceived = Signal(dict)
    modulesReceived = Signal(dict)
    attached, root = True, None

    def __init__(self):
        super().__init__()
        self.calls, self.jobs = [], []
        self.state = {
            "core": {
                "epoch": "core",
                "capabilities": {
                    "margin_trading": True,
                    "margin_simulation": True,
                    "margin_cost_policy": True,
                    "margin_cost_policy_scope": COST_SCOPE,
                },
            }
        }

    def request(self, method, path, payload=None, *, tag):
        self.calls.append((method, path, deepcopy(payload), tag))

    def submit_job(self, kind, parameters):
        self.jobs.append((kind, deepcopy(parameters)))
        return "a" * 32


def registry(capabilities=None, epoch="core"):
    return {
        "core_epoch": epoch,
        "state": "ready",
        "accounts": [],
        "operations": [],
        "capabilities": {"cost_policy": True, "cost_policy_scope": COST_SCOPE}
        if capabilities is None
        else capabilities,
    }


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    value = QApplication.instance() or QApplication(["cost-controls"])
    configure_fonts(value)
    return value


@pytest.fixture
def page(app):
    value = MarginPage(Client())
    value.timer.stop()
    value.setStyleSheet(stylesheet("light"))
    value.update_state(value.client.state)
    value.apply_snapshot(registry())
    for control in (value.start, value.bracket_time):
        control.setDateTime(QDateTime.fromMSecsSinceEpoch(0, QTimeZone(b"UTC")))
    value.module.setCurrentIndex(value.module.findData("target_quantity"))
    value.save_strategy()
    value.manual_assumptions.setChecked(True)
    yield value
    value.close()
    value.deleteLater()
    app.processEvents()


def replay_draft(page):
    parameters = page.parameters()
    result = {
        key: deepcopy(parameters[key]) for key in ("account", "strategies", "start_ms", "brackets")
    }
    result.update(
        spec={"fee_rate": parameters["fee_rate"], "backend": "cpu"},
        allow_noncausal_research_input=False,
    )
    if "cost_policy" in parameters:
        result["cost_policy"] = parameters["cost_policy"]
    return result


def test_default_off_and_explicit_toggle_preserve_legacy_request_and_raw_strings(page):
    legacy = page.parameters()
    assert "cost_policy" not in legacy and not page.cost_policy.isChecked()
    page.cost_policy.load_document(policy())
    assert page.parameters()["cost_policy"] == policy()
    assert page.cost_policy.entry_delay.value() == 1
    assert page.cost_policy.reduction_delay.value() == 0
    assert "1ms" in page.cost_policy.note.text() and "손절" in page.cost_policy.note.text()
    page.cost_policy.setChecked(False)
    assert page.parameters() == legacy
    page.cost_policy.setChecked(True)
    assert page.parameters()["cost_policy"]["slippage_rate"] == "0.0100"
    page.configure()
    posts = [row for row in page.client.calls if row[0] == "POST"]
    assert len(posts) == 1 and posts[0][2]["parameters"]["cost_policy"] == policy()


@pytest.mark.parametrize(
    "change",
    [
        {"entry_delay_ms": True},
        {"schema_version": True},
        {"model": "legacy"},
        {"slippage_rate": "0.250000000000000001"},
        {"mark_drift_bound_rate": "1E-19"},
        {"reduction_delay_ms": 86400001},
    ],
)
def test_invalid_loaded_policy_preserves_entire_previous_draft(page, change):
    page.cost_policy.load_document(policy())
    before = page.parameters()
    with pytest.raises(ValueError):
        page.cost_policy.load_document(policy(**change))
    assert page.parameters() == before and page.cost_policy.isChecked()


def test_structured_limits_native_delays_and_invalid_manual_input_block_submit(page):
    page.cost_policy.load_document(
        policy(
            slippage_rate="0.25",
            execution_price_bound_rate="0.5",
            mark_drift_bound_rate="0.5",
            entry_delay_ms=86400000,
        )
    )
    assert page.cost_policy.entry_delay.maximum() == 86400000
    assert type(page.parameters()["cost_policy"]["entry_delay_ms"]) is int
    page.cost_policy.slippage.setText("NaN")
    assert not page.configure_button.isEnabled()
    page.configure()
    assert not any(row[0] == "POST" for row in page.client.calls)
    page.cost_policy.slippage.setText("0.250000000000000001")
    assert not page.cost_policy.can_submit()


@pytest.mark.parametrize(
    "capabilities",
    [
        {},
        {"cost_policy": 1, "cost_policy_scope": COST_SCOPE},
        {"cost_policy": True, "cost_policy_scope": "wrong"},
        {"cost_policy": "true", "cost_policy_scope": COST_SCOPE},
    ],
)
def test_capability_loss_preserves_selection_and_blocks_without_dropping_policy(page, capabilities):
    legacy = page.parameters()
    page.cost_policy.load_document(policy())
    page.apply_snapshot(registry(capabilities))
    assert page.cost_policy.isChecked() and page.cost_policy.slippage.text() == "0.0100"
    assert not page.configure_button.isEnabled()
    with pytest.raises(ValueError, match="selection retained"):
        page.parameters()
    assert not any(row[0] == "POST" for row in page.client.calls)
    page.cost_policy.setChecked(False)
    assert page.parameters() == legacy


def test_epoch_and_disconnect_invalidation_preserve_checked_policy(page):
    page.cost_policy.load_document(policy())
    page.set_attached(False)
    assert page.cost_policy.isChecked() and not page.cost_policy.supported
    page.set_attached(True)
    page.update_state({"core": {"epoch": "next", "capabilities": {"margin_trading": True}}})
    assert page.cost_policy.isChecked() and not page.configure_button.isEnabled()
    assert page.cost_policy.slippage.text() == "0.0100"


def test_funding_and_cost_toggles_do_not_silently_disable_each_other(page):
    page.funding.set_supported(True)
    schedule = {
        "schema_version": 1,
        "model": "linear_usdt_scenario_rate_v1",
        "points": [
            {
                "instrument": BTC.key,
                "event_time_ms": 0,
                "source_available_at_ms": 0,
                "reference_time_ms": 0,
                "rate": "0",
                "reference_price": "100",
            }
        ],
    }
    page.funding.load_document(schedule)
    page.cost_policy.load_document(policy())
    both = page.parameters()
    assert both["funding"] == schedule and both["cost_policy"] == policy()
    page.cost_policy.setChecked(False)
    assert page.parameters()["funding"] == schedule and "cost_policy" not in page.parameters()
    page.cost_policy.setChecked(True)
    page.funding.setChecked(False)
    assert page.parameters()["cost_policy"] == policy() and "funding" not in page.parameters()


def test_replay_copy_validates_before_commit_and_legacy_copy_resets_policy(page, app):
    legacy = replay_draft(page)
    page.cost_policy.load_document(policy())
    recorded = replay_draft(page)
    form = MarginReplayControls()
    try:
        form.cost_policy.set_supported(True)
        form.load_parameters(recorded)
        assert form.cost_policy.document() == policy() and not form.manual.isChecked()
        form.manual.setChecked(True)
        before = form.parameters()
        invalid = deepcopy(recorded)
        invalid["account"]["balance"] = "1234"
        invalid["cost_policy"]["entry_delay_ms"] = True
        with pytest.raises(ValueError):
            form.load_parameters(invalid)
        assert form.parameters() == before
        form.load_parameters(legacy)
        assert not form.cost_policy.isChecked() and "cost_policy" not in form.parameters(
            _preview=True
        )
        assert not form.manual.isChecked() and not form.noncausal.isChecked()
    finally:
        form.close()
        form.deleteLater()


def test_research_native_cap_loss_blocks_shared_submit_without_implicit_toggle(page, app):
    client = Client()
    research = ResearchPage(client, ["simulate"])
    research.simulation_mode.setCurrentIndex(research.simulation_mode.findData("margin"))
    page.cost_policy.load_document(policy())
    research.margin_replay.load_parameters(replay_draft(page))
    research.margin_replay.manual.setChecked(True)
    assert research.run_button.isEnabled()
    client.state["core"]["capabilities"]["margin_cost_policy"] = 1
    research.set_attached(True)
    assert not research.run_button.isEnabled() and research.margin_replay.cost_policy.isChecked()
    assert research.margin_replay.cost_policy.slippage.text() == "0.0100"
    research.margin_replay.cost_policy.setChecked(False)
    assert research.run_button.isEnabled() and client.jobs == []
    research.close()


def quote_result():
    raw = policy()
    parsed = MarginCostPolicy.parse(raw)
    fact = FillEvent("fill", "order", BTC, "ts", D(1), D(101), D(".101"), "USDT", 120000, 180000)
    return {
        "schema_version": 1,
        "state": "complete",
        "quote_asset": "USDT",
        "initial_wallet": "1000",
        "scope": "virtual_margin_v2_ohlcv_proxy",
        "wallet": "999.899",
        "positions": {BTC.key: "1"},
        "equity": [{"timestamp_ms": 180000, "wallet": "999.899", "equity": "999.899"}],
        "fills": [_document(fact) | {"timestamp_ms": 120000, "quantity": "1", "realized_pnl": "0"}],
        "events": [],
        "cost_policy": {
            "schema_version": 1,
            "model": raw["model"],
            "raw_document": raw,
            "canonical_document": parsed.document(),
            "policy_sha256": parsed.sha256,
            "request_sha256": sha256(
                json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        },
        "cost_quotes": {
            "schema_version": 1,
            "model": raw["model"],
            "state": "ready",
            "total": 1,
            "truncated": False,
            "rows": [
                {
                    "fill_id": fact.fill_id,
                    "order_id": fact.order_id,
                    "instrument": BTC.key,
                    "side": "BUY",
                    "quantity": "1",
                    "raw_price": "100",
                    "final_price": "101",
                    "fee": "0.101",
                    "explanatory_slippage_amount": "1",
                    "policy_sha256": parsed.sha256,
                    "issued_at_ms": 60000,
                    "eligible_at_ms": 120000,
                    "event_time_ms": fact.event_time_ms,
                    "available_at_ms": fact.available_at_ms,
                }
            ],
        },
    }


def test_result_view_shows_only_matching_confirmed_quotes_and_preserves_legacy_view(app):
    view = SimulationResultsPage(Client())
    record = quote_result()
    summary = summarize_simulation(record)
    packet = {"path": "fixture.json", "sha256": "a" * 64, "summary": summary}
    view.accept_result(packet)
    assert view.cost_quotes.rowCount() == 1
    assert [view.cost_quotes.item(0, column).text() for column in (4, 5, 6, 7)] == [
        "100",
        "101",
        "0.101",
        "1",
    ]
    assert summary["reconciliation"]["matches"] is True
    view.tabs.setCurrentIndex(2)
    view.show()
    for width in (1360, 800):
        view.resize(width, 950)
        app.processEvents()
        assert view.cost_quote_group.y() > view.fill_count.geometry().bottom()
        assert view.cost_quotes.horizontalScrollBar().maximum() > 0
    malformed = deepcopy(packet)
    malformed["summary"]["cost_quotes"]["rows"][0]["fee"] = "99"
    view.accept_result(malformed)
    assert view.summary is None and view.cost_quotes.rowCount() == 0
    assert "불일치" in view.status.text()
    del record["cost_quotes"], record["cost_policy"]
    view.accept_result(
        {"path": "legacy.json", "sha256": "b" * 64, "summary": summarize_simulation(record)}
    )
    assert view.cost_quote_group.isHidden() and view.cost_quotes.rowCount() == 0
    view.close()


def test_result_copy_restores_raw_policy_and_requires_new_consent_without_submit(page, app):
    page.cost_policy.load_document(policy())
    draft = replay_draft(page)
    client = Client()
    research = ResearchPage(client, ["simulate"])
    view = SimulationResultsPage(client)
    view.accept_result(
        {
            "path": "fixture.json",
            "sha256": "a" * 64,
            "summary": summarize_simulation(quote_result()),
        }
    )
    view._copy_parameters = draft
    view._update_copy_button()
    view.configurationDraft.connect(research.apply_simulation_draft)
    view.copy_button.click()
    assert research.margin_replay.cost_policy.document() == policy()
    assert (
        not research.margin_replay.manual.isChecked()
        and not research.margin_replay.noncausal.isChecked()
    )
    assert client.jobs == [] and not any(row[0] == "POST" for row in client.calls)
    view.close()
    research.close()


@pytest.mark.parametrize("failure", ["invalid_policy", "capability_lost"])
def test_failed_research_copy_keeps_dataset_form_and_original_consent_atomic(page, app, failure):
    page.cost_policy.load_document(policy())
    original = replay_draft(page) | {"dataset": "dataset-A"}
    client = Client()
    research = ResearchPage(client, ["simulate"])
    research.apply_simulation_draft(original)
    controls = research.margin_replay
    controls.manual.setChecked(True)
    controls.noncausal.setChecked(True)
    before = controls.parameters()
    proposed = deepcopy(original)
    proposed["dataset"] = "dataset-B"
    proposed["account"]["balance"] = "1234"
    proposed["cost_policy"]["entry_delay_ms"] = 10
    if failure == "invalid_policy":
        proposed["cost_policy"]["entry_delay_ms"] = True
    else:
        client.state["core"]["capabilities"]["margin_cost_policy"] = False
        research.set_attached(True)
    with pytest.raises(ValueError):
        research.apply_simulation_draft(proposed)
    assert research.dataset.text() == "dataset-A"
    assert research.simulation_mode.currentData() == "margin"
    assert controls.manual.isChecked() and controls.noncausal.isChecked()
    assert controls.cost_policy.isChecked() and controls.cost_policy.entry_delay.value() == 1
    client.state["core"]["capabilities"]["margin_cost_policy"] = True
    research.set_attached(True)
    assert controls.parameters() == before
    assert client.jobs == [] and not any(row[0] == "POST" for row in client.calls)
    research.close()


def test_quote_preview_is_recent_and_bounded_while_original_result_stays_complete(app):
    record = quote_result()
    fill, quote = record["fills"][0], record["cost_quotes"]["rows"][0]
    count = COST_QUOTE_PREVIEW_ROWS + 1
    record["fills"] = [
        fill | {"fill_id": f"fill{i}", "order_id": f"order{i}"} for i in range(count)
    ]
    record["cost_quotes"]["rows"] = [
        quote | {"fill_id": f"fill{i}", "order_id": f"order{i}"} for i in range(count)
    ]
    record["cost_quotes"]["total"] = count
    record["wallet"] = str(D(1000) - D(".101") * count)
    record["equity"][0]["wallet"] = record["wallet"]
    record["equity"][0]["equity"] = record["wallet"]
    summary = summarize_simulation(record)
    view = SimulationResultsPage(Client())
    view.accept_result({"path": "fixture.json", "sha256": "a" * 64, "summary": summary})
    assert view.cost_quotes.rowCount() == COST_QUOTE_PREVIEW_ROWS
    assert view.cost_quotes.item(0, 11).text() == "fill1"
    assert f"total {count}" in view.cost_quote_count.text()
    assert len(summary["cost_quotes"]["rows"]) == count
    assert record["cost_quotes"]["rows"][0]["fill_id"] == "fill0"
    view.close()


def test_cost_request_capacity_preserves_previous_loaded_policy(app):
    editor = CostEditor(max_request_bytes=1024)
    editor.set_supported(True)
    editor.load_document(policy())
    before = editor.document()
    editor.validate_candidate = lambda value: {"cost_policy": value, "padding": "x" * 1024}
    with pytest.raises(ValueError, match="previous draft preserved"):
        editor.load_document(policy(entry_delay_ms=10))
    assert editor.document() == before
    editor.close()


def test_cost_form_resize_does_not_overlap_following_controls_or_change_draft(page, app):
    page.cost_policy.load_document(policy())
    before = page.parameters()
    page.tabs.setCurrentIndex(1)
    page.show()
    content = page.cost_policy.parentWidget()
    for width in (1360, 800, 1360):
        page.resize(width, 950)
        app.processEvents()
        page.tabs.widget(1).ensureWidgetVisible(page.cost_policy)
        app.processEvents()
        assert page.cost_policy.geometry().bottom() < page.configure_button.y()
        controls = [
            page.cost_policy.slippage,
            page.cost_policy.execution_bound,
            page.cost_policy.mark_drift,
            page.cost_policy.entry_delay,
            page.cost_policy.reduction_delay,
        ]
        for first, second in zip(controls, controls[1:], strict=False):
            assert (
                first.mapTo(content, QPoint(0, first.height())).y()
                < second.mapTo(content, QPoint()).y()
            )
        assert page.parameters() == before
