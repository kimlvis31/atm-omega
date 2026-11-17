"""GET-only registered settings reuse, atomic drafts and fresh ordinary-configure consent."""

import os
from copy import deepcopy

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QDateTime, QTimeZone
from PySide6.QtWidgets import QApplication
from test_margin_page import Client, account, posts, registry, save_target, state

from omega.domain.external_features import ExternalFeatureBinding
from omega.domain.models import Instrument, Market
from omega.gui.margin_page import MarginPage
from omega.gui.presentation import configure_fonts, stylesheet
from omega.runtime.margin_configuration import configuration_response

EPOCH = "1" * 32
START = 1700000040000


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    result = QApplication.instance() or QApplication(["configuration-copy"])
    configure_fonts(result)
    return result


@pytest.fixture
def page(app):
    result = MarginPage(Client())
    result.timer.stop()
    result.setStyleSheet(stylesheet("light"))
    result.update_state(state(EPOCH))
    result.set_modules({"ts": [{"id": "target_quantity"}, {"id": "tef"}]})
    result.apply_snapshot(registry(EPOCH, [account("source"), account("other")]))
    for edit in (result.start, result.bracket_time):
        edit.setDateTime(QDateTime.fromMSecsSinceEpoch(START, QTimeZone(b"UTC")))
    save_target(result, "old")
    result.account_id.setText("old-draft")
    result.balance.setText("1234")
    result.manual_assumptions.setChecked(True)
    result.account_table.selectRow(0)
    yield result
    result.close()
    result.deleteLater()
    app.processEvents()


def parameters():
    key = "binance:usdm:BTCUSDT"
    return {
        "account": {
            "account_id": "source",
            "balance": "1000",
            "positions": [
                {
                    "instrument": key,
                    "owner_id": "copied",
                    "margin_mode": "isolated",
                    "leverage": 10,
                    "budget_margin": "100",
                    "isolated_collateral": "150",
                }
            ],
        },
        "strategies": [
            {
                "id": "copied",
                "instrument": key,
                "module": "target_quantity",
                "configuration": {"quantity": "-1"},
                "analysis": {},
                "constraints": {},
            }
        ],
        "start_ms": START,
        "fee_rate": "0.0010",
        "brackets": [
            {
                "instrument": key,
                "observed_at_ms": START,
                "available_at_ms": START,
                "rows": [
                    {
                        "notional_floor": "0",
                        "notional_cap": "1000000",
                        "maintenance_rate": "0.005",
                        "maintenance_amount": "0",
                        "max_leverage": 20,
                    }
                ],
            }
        ],
    }


def packet(value=None, **changes):
    value = parameters() if value is None else value
    return configuration_response(value["account"]["account_id"], parameters=value) | {
        "core_epoch": EPOCH,
        **changes,
    }


def fetch(page, value=None):
    page.copy_configuration_button.click()
    method, path, payload, tag = page.client.calls[-1]
    assert method == "GET" and path.endswith("/source/configuration") and payload is None
    page.client.resultReceived.emit(tag, packet(value))
    return tag


def target(page, *, identity="new-account", capital="2000", stamp=START + 60000):
    page.config_copy.new_id.setText(identity)
    page.config_copy.capital.setText(capital)
    page.config_copy.start.setDateTime(QDateTime.fromMSecsSinceEpoch(stamp, QTimeZone(b"UTC")))
    page.config_copy.explicit_start.setChecked(True)


def frozen(page):
    return (
        page.account_id.text(),
        page.balance.text(),
        page.start.dateTime().toMSecsSinceEpoch(),
        deepcopy(page.rows),
        page.configuration.toPlainText(),
        page.manual_assumptions.isChecked(),
        page.funding.isChecked(),
        deepcopy(page.funding.raw),
        page.cost_policy.isChecked(),
        page.cost_policy._raw(),
        dict(page.config_copy.requirements),
        {key: control.isChecked() for key, control in page.config_copy.consents.items()},
    )


def test_get_only_staging_distinct_explicit_target_and_ordinary_paused_creation(page):
    before = frozen(page)
    source = parameters()
    original = deepcopy(source)
    fetch(page, source)
    assert frozen(page) == before and posts(page) == []
    assert page.config_copy.new_id.text() == page.config_copy.capital.text() == ""
    assert not page.config_copy.explicit_start.isChecked()
    page.config_copy.apply_button.click()
    assert frozen(page) == before
    target(page)
    page.config_copy.apply_button.click()
    assert page.account_id.text() == "new-account" and page.balance.text() == "2000"
    assert page.rows[0]["strategy"]["id"] == "copied"
    assert page.rows[0]["position"]["isolated_collateral"] == "150"
    assert not page.manual_assumptions.isChecked() and source == original
    assert posts(page) == [] and page.client.commands == []
    page.manual_assumptions.setChecked(True)
    draft = page.parameters()
    assert draft["account"]["account_id"] == "new-account" and draft["start_ms"] == START + 60000
    assert "external_feature_wait_ms" not in draft and "funding" not in draft
    page.configure_button.click()
    assert len(posts(page)) == 1 and posts(page)[0][2]["kind"] == "configure"
    assert page.accounts["source"]["account"]["account"]["cross_wallet"] == "899.9"


@pytest.mark.parametrize(
    "identity,capital,stamp",
    [
        ("source", "2000", START),
        ("other", "2000", START),
        ("bad/id", "2000", START),
        ("new", "", START),
        ("new", "-1", START),
        ("new", "149", START),
        ("new", "2000", START + 1),
        ("new", "2000", START - 120000),
    ],
)
def test_invalid_target_preserves_entire_old_draft(page, identity, capital, stamp):
    before = frozen(page)
    fetch(page)
    target(page, identity=identity, capital=capital, stamp=stamp)
    page.config_copy.apply_button.click()
    assert frozen(page) == before and posts(page) == []
    assert "retained" in page.config_copy.status.text()


@pytest.mark.parametrize("change", ["hash", "identity", "unavailable", "epoch", "failure"])
def test_bad_or_unknown_get_keeps_previous_draft_and_consents(page, change):
    before = frozen(page)
    page.copy_configuration_button.click()
    tag = page.client.calls[-1][3]
    data = packet()
    if change == "hash":
        data["configuration_sha256"] = "a" * 64
    elif change == "identity":
        data["account_id"] = "other"
    elif change == "unavailable":
        data = configuration_response("source", reason_code="configuration_unconfirmed") | {
            "core_epoch": EPOCH
        }
    elif change == "epoch":
        data["core_epoch"] = "2" * 32
    if change == "failure":
        page.client.failed.emit(tag, "timeout")
    else:
        page.client.resultReceived.emit(tag, data)
    assert frozen(page) == before and page.config_copy.source is None
    assert not page.config_copy.apply_button.isEnabled() and posts(page) == []


@pytest.mark.parametrize("change", ["selection", "epoch", "disconnect"])
def test_late_selection_epoch_disconnect_response_cannot_replace_draft(page, change):
    before = frozen(page)
    page.copy_configuration_button.click()
    tag = page.client.calls[-1][3]
    if change == "selection":
        page.account_table.selectRow(1)
    elif change == "epoch":
        page.update_state(state("2" * 32))
    else:
        page.client.attached = False
        page.client.connectionChanged.emit(False)
    page.client.resultReceived.emit(tag, packet())
    assert frozen(page) == before and page.config_copy.source is None and posts(page) == []


@pytest.mark.parametrize(
    "change", ["multi", "offset", "analysis", "constraint", "no_tier", "source_meta"]
)
def test_unrepresentable_configuration_explicitly_rejected_without_flattening(page, change):
    source = parameters()
    if change == "multi":
        source["brackets"][0]["rows"].append(deepcopy(source["brackets"][0]["rows"][0]))
    elif change == "offset":
        source["brackets"][0]["rows"][0]["maintenance_amount"] = "1"
    elif change == "analysis":
        source["strategies"][0]["analysis"] = {"custom": 42}
    elif change == "constraint":
        source["strategies"][0]["constraints"] = {"quantity_origin": "0.0001"}
    elif change == "no_tier":
        source["brackets"] = []
    else:
        source["brackets"][0]["source"] = "venue"
    before = frozen(page)
    fetch(page, source)
    assert frozen(page) == before and page.config_copy.source is None and posts(page) == []
    assert "retained" in page.config_copy.status.text()


def costs():
    return {
        "schema_version": 1,
        "model": "linear_usdt_ohlcv_cost_v1",
        "slippage_rate": "0.0100",
        "execution_price_bound_rate": "0.1",
        "mark_drift_bound_rate": "0.1",
        "entry_delay_ms": 1,
        "reduction_delay_ms": 0,
        "limit_price_policy": "adverse_then_cap_v1",
    }


def funding():
    return {
        "schema_version": 1,
        "model": "linear_usdt_scenario_rate_v1",
        "points": [
            {
                "instrument": "binance:usdm:BTCUSDT",
                "event_time_ms": START + 120000,
                "source_available_at_ms": START + 120000,
                "reference_time_ms": START + 120000,
                "rate": "-0.0100",
                "reference_price": "100",
            }
        ],
    }


def test_policies_preserved_visible_with_separate_unchecked_fresh_consents(page):
    page.funding.set_supported(True)
    page.cost_policy.set_supported(True)
    source = parameters() | {"funding": funding(), "cost_policy": costs()}
    fetch(page, source)
    target(page)
    page.config_copy.apply_button.click()
    assert page.funding.isChecked() and page.cost_policy.isChecked()
    assert page.funding.document() == funding() and page.cost_policy.document() == costs()
    assert not page.config_copy.consents["funding"].isChecked()
    assert not page.config_copy.consents["cost"].isChecked()
    page.manual_assumptions.setChecked(True)
    assert not page.configure_button.isEnabled() and posts(page) == []
    for control in page.config_copy.consents.values():
        control.setChecked(True)
    assert page.parameters()["cost_policy"] == costs()


def test_failed_second_copy_preserves_applied_first_copy_and_checked_consents(page):
    page.funding.set_supported(True)
    page.cost_policy.set_supported(True)
    fetch(page, parameters() | {"funding": funding(), "cost_policy": costs()})
    target(page)
    page.config_copy.apply_button.click()
    page.manual_assumptions.setChecked(True)
    for control in page.config_copy.consents.values():
        control.setChecked(True)
    before = frozen(page)
    invalid = parameters()
    invalid["brackets"][0]["rows"].append(deepcopy(invalid["brackets"][0]["rows"][0]))
    fetch(page, invalid)
    assert frozen(page) == before and posts(page) == []


def test_user_explicitly_turning_copied_policies_off_omits_keys_without_unused_consent(page):
    page.funding.set_supported(True)
    page.cost_policy.set_supported(True)
    fetch(page, parameters() | {"funding": funding(), "cost_policy": costs()})
    target(page)
    page.config_copy.apply_button.click()
    page.funding.setChecked(False)
    page.cost_policy.setChecked(False)
    page.manual_assumptions.setChecked(True)
    assert "funding" not in page.parameters() and "cost_policy" not in page.parameters()
    assert not page.config_copy.consents["funding"].isChecked()
    assert posts(page) == []


def test_second_selected_source_can_be_read_before_late_first_response(page):
    before = frozen(page)
    page.copy_configuration_button.click()
    first_tag = page.client.calls[-1][3]
    page.account_table.selectRow(1)
    assert page.copy_configuration_button.isEnabled()
    page.copy_configuration_button.click()
    second_tag = page.client.calls[-1][3]
    other = parameters()
    other["account"]["account_id"] = "other"
    page.client.resultReceived.emit(second_tag, packet(other))
    assert page.config_copy.source["source_id"] == "other"
    page.client.resultReceived.emit(first_tag, packet())
    assert page.config_copy.source["source_id"] == "other"
    assert frozen(page) == before and posts(page) == []


def test_two_matching_instrument_tiers_preserve_rows_and_different_tiers_reject(page):
    source = parameters()
    source["account"]["positions"].append(
        {
            **source["account"]["positions"][0],
            "instrument": "binance:usdm:ETHUSDT",
            "owner_id": "second",
        }
    )
    source["strategies"].append(
        {
            **source["strategies"][0],
            "id": "second",
            "instrument": "binance:usdm:ETHUSDT",
        }
    )
    source["brackets"].append(deepcopy(source["brackets"][0]))
    source["brackets"][1]["instrument"] = "binance:usdm:ETHUSDT"
    fetch(page, source)
    target(page)
    page.config_copy.apply_button.click()
    assert len(page.rows) == 2 and [row["strategy"]["id"] for row in page.rows] == [
        "copied",
        "second",
    ]
    before = frozen(page)
    source["brackets"][1]["rows"][0]["notional_cap"] = "2000000"
    fetch(page, source)
    assert frozen(page) == before and page.config_copy.source is None


def test_module_retirement_or_registry_unknown_before_apply_preserves_old_draft(page):
    fetch(page)
    target(page)
    before = frozen(page)
    page._runtime_ready = False
    page.apply_configuration_copy()
    assert frozen(page) == before
    page._runtime_ready = True
    page.set_modules({"ts": [{"id": "different", "configuration_schema": {}}]})
    # A module-list update can change the open editor; capture that independent UI change.
    current = frozen(page)
    page.apply_configuration_copy()
    assert frozen(page) == current and posts(page) == []


def test_target_to_tef_copy_allows_period_edit_save_and_target_disables_again(page):
    periods = (page.ema, page.fast, page.slow, page.signal)
    assert not any(control.isEnabled() for control in periods)
    source = parameters()
    source["strategies"][0].update(
        module="tef",
        configuration={"model": "MMACDDEFAULT"},
        analysis={"ema_period": 20, "mmacd_periods": [12, 26], "signal_period": 9},
    )
    fetch(page, source)
    target(page)
    page.config_copy.apply_button.click()
    assert all(control.isEnabled() for control in periods)
    for control, value in zip(periods, (21, 13, 27, 10), strict=True):
        control.setValue(value)
    page.save_strategy()
    assert page.rows[0]["strategy"]["analysis"] == {
        "ema_period": 21,
        "mmacd_periods": [13, 27],
        "signal_period": 10,
    }
    fetch(page)
    target(page, identity="target-again")
    page.config_copy.apply_button.click()
    assert not any(control.isEnabled() for control in periods)
    assert posts(page) == []


@pytest.mark.parametrize("policy", ["funding", "cost_policy"])
def test_capability_loss_between_get_and_apply_preserves_previous_draft(page, policy):
    page.funding.set_supported(True)
    page.cost_policy.set_supported(True)
    source = parameters() | {policy: funding() if policy == "funding" else costs()}
    fetch(page, source)
    before = frozen(page)
    target(page)
    (page.funding if policy == "funding" else page.cost_policy).set_supported(False)
    page.config_copy.apply_button.click()
    assert frozen(page) == before and posts(page) == []


def test_saved_nn_binding_is_not_authority_and_fresh_current_selection_is_required(page):
    schema = {
        "properties": {
            "signal_field": {
                "type": "string",
                "default": "NN.score",
                "x-omega-external-feature": {"namespace": "NN", "required": True},
            }
        }
    }
    page.set_modules({"ts": [{"id": "neural_signal", "configuration_schema": schema}]})
    binding = ExternalFeatureBinding(
        "causal_nn",
        "model",
        "main",
        Instrument("binance", Market.USDM, "BTCUSDT"),
        "a" * 64,
        "b" * 64,
        ("NN.score",),
    )
    source = parameters()
    source["strategies"][0].update(
        module="neural_signal",
        configuration={"signal_field": "NN.score"},
        external_features=[binding.document()],
    )
    source["external_feature_wait_ms"] = 12345
    fetch(page, source)
    target(page)
    page.config_copy.apply_button.click()
    assert page.account_id.text() == "new-account"
    assert (
        page.external_inputs.received_at is None
        and page.external_inputs.selection.currentData() is None
    )
    assert page.external_feature_wait.value() == 12345
    assert not page.config_copy.consents["nn"].isChecked() and posts(page) == []
    page.config_copy.consents["nn"].setChecked(True)
    page.manual_assumptions.setChecked(True)
    with pytest.raises(ValueError, match="Refresh"):
        page.parameters()
    page.external_inputs.apply_snapshot(
        {
            "bindings": [binding.document()],
            "server_time_ms": START,
            "core_epoch": EPOCH,
            "strategy_delivery_enabled": True,
            "strategy_delivery_scope": "virtual_margin_v2_opt_in",
            "live_orders_enabled": False,
        },
        EPOCH,
    )
    page.external_inputs.select_documents([binding.document()])
    assert page.parameters()["strategies"][0]["external_features"] == [binding.document()]
    assert posts(page) == [] and page.client.commands == []


@pytest.mark.parametrize("width", [800, 1360])
def test_structured_copy_panel_resizes_and_starts_collapsed(page, app, width):
    assert not page.config_copy.isChecked()
    fetch(page)
    page.resize(width, 850)
    page.show()
    app.processEvents()
    assert page.width() == width
    assert page.config_copy.new_id.geometry().width() > 0
