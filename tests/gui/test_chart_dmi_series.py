"""Core-produced retrospective DMI chart series; no copied formula or real API."""

import importlib.util
import json
import os
from copy import deepcopy
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtGui import QFont, QFontDatabase
    from PySide6.QtWidgets import QApplication

    value = QApplication.instance() or QApplication(["chart-dmi-tests"])
    font = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts/segoeui.ttf"
    if not QFontDatabase.families() and font.is_file():
        registered = QFontDatabase.addApplicationFont(str(font))
        value.setFont(QFont(QFontDatabase.applicationFontFamilies(registered)[0], 10))
    return value


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

        def request(self, method, path, payload=None, *, tag):
            self.calls.append((method, path, deepcopy(payload), tag))

    return Client()


def rows(count=30, begin=0):
    from omega.gui.models import Candle

    return tuple(
        Candle((begin + i) * 60000, 10, 13 + i % 4, 9, 11 + i % 2, 5) for i in range(count)
    )


def raw(page, values=None, source="database"):
    page.accept_candles(
        page.panes[0].selection.text(),
        {
            "candles": [asdict(row) for row in (values if values is not None else rows())],
            "source": source,
        },
    )


@pytest.fixture
def page(app, tmp_path):
    from omega.gui.charts_page import ChartsPage

    value = ChartsPage(client(), root=tmp_path)
    value.resize(1360, 900)
    value.show()
    value.set_presentation("light", "en")
    value.update_state({"core": {"epoch": "a", "capabilities": {"chart_series": True}}})
    raw(value)
    app.processEvents()
    value.client.calls.clear()
    yield value
    value.close()
    value.deleteLater()
    app.processEvents()


def enable(page, period=1):
    pane = page.panes[0]
    pane.set_timeframe(period)
    profile = deepcopy(pane.profile)
    profile["enabled"]["DMIxADX"] = True
    profile["dmi_period"] = 3
    profile["slots"][0]["indicator"] = "DMIxADX"
    pane.apply_profile(profile)
    return page.client.calls[-1] if page.client.calls else None


def response(post, state="complete", **changes):
    params = post[2]["parameters"]
    width = params["period_minutes"] * 60000
    opening = (params["start_ms"] + width - 1) // width * width
    produced = params["end_ms"] + 12000
    result = {
        "instrument": params["instrument"],
        "period_minutes": params["period_minutes"],
        "nSamples": params["nSamples"],
        "source": "database",
        "as_of_ms": produced - 1000,
        "produced_at_ms": produced,
        "source_checksum": "c" * 64,
        "knowledge_mode": "retrospective_display",
        "quality": {"status": "observed"},
        "rows": [
            {
                "open_time_ms": stamp,
                "close_time_ms": stamp + width,
                "available_at_ms": produced,
                "DMIxADX": None if stamp == opening else stamp / 60000,
                "ABSMA": 5.0,
                "ABSMAREL": 2.0,
            }
            for stamp in range(opening, params["end_ms"] - width + 1, width)
        ],
    }
    row = {
        "id": post[2]["id"],
        "parameters": params,
        "state": state,
        "spec_hash": sha256(
            json.dumps(params, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest(),
        "core_epoch": "a",
        "result": result if state == "complete" else None,
    }
    row.update(changes)
    return row


def emit(page, post, body=None):
    page.client.resultReceived.emit(post[3], body or response(post))


def test_v1_migration_preserves_three_slots_and_no_automatic_file_write(page, app):
    from omega.gui.chart_profiles import default_profile, validate_profile
    from omega.gui.charts_page import ChartsPage

    old = default_profile()
    for key in ("schema_version", "dmi_period", "dmi_display", "auxiliary", "psar", "swing"):
        old.pop(key)
    old["enabled"] = {code: old["enabled"][code] for code in ("VOL", "MFI", "MMACD")}
    old["slots"] = list(reversed(old["slots"]))
    before = deepcopy(old)
    migrated = validate_profile(old)
    assert old == before and migrated["schema_version"] == 6
    assert migrated["slots"] == old["slots"] and not migrated["enabled"]["DMIxADX"]
    doc = page.workspace_snapshot()
    doc["panes"][0]["profile"] = old
    page.store.path.parent.mkdir(parents=True, exist_ok=True)
    page.store.path.write_text(json.dumps(doc), encoding="utf-8")
    encoded = page.store.path.read_bytes()
    restored = ChartsPage(client(), root=page.store.path.parent.parent)
    assert restored.panes[0].profile["schema_version"] == 6
    assert page.store.path.read_bytes() == encoded
    assert len(restored.panes[0].si_plots) == 3
    restored.close()
    restored.deleteLater()
    app.processEvents()


def test_structured_settings_extended_choices_three_slots_and_save(page):
    editor = page.panes[0].settings_panel
    assert len(editor.allocations) == 3
    assert {editor.allocations[0].itemText(i) for i in range(editor.allocations[0].count())} == {
        "VOL",
        "MFI",
        "MMACD",
        "DMIxADX",
        "OPENINTEREST",
        "LONGSHORTRATIO",
        "AGGTRADE",
        "DEPTH",
        "FUNDING",
        "NES",
        "TPD",
    }
    editor.enabled["DMIxADX"].setChecked(True)
    editor.dmi_period.setValue(3)
    editor.allocations[0].setCurrentText("DMIxADX")
    assert editor.apply()
    assert page.panes[0].si_plots[0].code == "DMIxADX"
    assert not page.panes[0].volume.isEnabled()
    page.save_workspace()
    assert page.store.load()["panes"][0]["profile"]["dmi_period"] == 3


def test_request_uses_raw_one_minute_range_not_aggregated_bars(page):
    post = enable(page, 5)
    assert post[0:2] == ("POST", "/api/chart-series")
    assert post[2]["parameters"] == {
        "instrument": "binance:usdm:BTCUSDT",
        "start_ms": 0,
        "end_ms": 1800000,
        "period_minutes": 5,
        "kind": "DMIxADX",
        "nSamples": 3,
    }
    assert len(page.panes[0].chart.candles) == 6
    emit(page, post)
    pane = page.panes[0]
    assert pane._dmi_state == "complete"
    assert pane.si_plots[0].lines["DMIxADX"] == (None, 5.0, 10.0, 15.0, 20.0, 25.0)
    assert "not knowledge at the historical close" in pane.producer_status.text()


def test_response_exact_timestamp_mapping_and_missing_rows_stay_none(page):
    post = enable(page)
    value = response(post)
    value["result"]["rows"] = value["result"]["rows"][1::2]
    emit(page, post, value)
    line = page.panes[0].si_plots[0].lines["DMIxADX"]
    assert len(line) == 30 and line[0] is None and line[1] == 1.0 and line[2] is None


def test_display_only_changes_have_zero_http_and_zero_numeric_recalculation(page, monkeypatch):
    from omega.gui import charts_page

    post = enable(page)
    emit(page, post)
    calls = []
    original = charts_page.display_indicators
    monkeypatch.setattr(
        charts_page,
        "display_indicators",
        lambda *args, **kwargs: (calls.append(1), original(*args, **kwargs))[1],
    )
    page.client.calls.clear()
    pane = page.panes[0]
    profile = deepcopy(pane.profile)
    profile.update(dmi_display="ABSMA", timezone="UTC+09:00", ema=True)
    profile["slots"] = list(reversed(profile["slots"]))
    pane.apply_profile(profile)
    assert calls == [] and page.client.calls == []
    assert pane.si_plots[2].lines["ABSMA"] == (5.0,) * 30


def test_numeric_period_data_and_calculation_switch_control_new_requests(page):
    post = enable(page)
    emit(page, post)
    pane = page.panes[0]
    profile = deepcopy(pane.profile)
    profile["dmi_period"] = 4
    pane.apply_profile(profile)
    second = page.client.calls[-1]
    assert second[0] == "POST" and second[2]["id"] != post[2]["id"]
    raw(page, rows(31))
    third = page.client.calls[-1]
    assert third[2]["parameters"]["end_ms"] == 1860000
    assert third[2]["id"] != second[2]["id"]
    profile["enabled"]["DMIxADX"] = False
    pane.apply_profile(profile)
    count = len(page.client.calls)
    pane.apply_profile(profile)
    assert len(page.client.calls) == count
    profile["enabled"]["DMIxADX"] = True
    pane.apply_profile(profile)
    assert page.client.calls[-1][0] == "POST"


def test_unknown_post_only_polls_same_id_without_reposting(page):
    post = enable(page)
    pane = page.panes[0]
    page.client.failed.emit(post[3], "timeout")
    pane._poll_dmi()
    get = page.client.calls[-1]
    assert get[0:2] == ("GET", "/api/chart-series/" + post[2]["id"])
    assert len([call for call in page.client.calls if call[0] == "POST"]) == 1
    emit(page, get, response(post))
    assert pane._dmi_state == "complete"


@pytest.mark.parametrize("period", [4320, "1w", "1M"])
def test_unsupported_period_never_posts_or_fills_zero(page, period):
    enable(page, period)
    pane = page.panes[0]
    assert not any(call[1] == "/api/chart-series" for call in page.client.calls)
    assert pane._dmi_state == "period_unavailable"
    assert all(value is None for value in pane.si_plots[0].lines["DMIxADX"])


def test_demo_source_never_mixes_database_numbers(page):
    raw(page, source="demo")
    enable(page)
    assert page.panes[0]._dmi_state == "source_unavailable"
    assert not any(call[1] == "/api/chart-series" for call in page.client.calls)


@pytest.mark.parametrize(
    "mutation",
    [
        "epoch",
        "parameters",
        "hash",
        "source",
        "asof",
        "available",
        "close",
        "duplicate",
        "nan",
        "missingfield",
    ],
)
def test_malformed_or_stale_result_never_reaches_plot(page, mutation):
    post = enable(page)
    value = response(post)
    if mutation == "epoch":
        value["core_epoch"] = "b"
    elif mutation == "parameters":
        value["parameters"] = {**value["parameters"], "nSamples": 9}
    elif mutation == "hash":
        value["spec_hash"] = "bad"
    elif mutation == "source":
        value["result"]["source"] = "demo"
    elif mutation == "asof":
        value["result"]["as_of_ms"] = value["result"]["produced_at_ms"] + 1
    elif mutation == "available":
        value["result"]["rows"][0]["available_at_ms"] = 60000
    elif mutation == "close":
        value["result"]["rows"][0]["close_time_ms"] += 1
    elif mutation == "duplicate":
        value["result"]["rows"].insert(1, deepcopy(value["result"]["rows"][0]))
    elif mutation == "nan":
        value["result"]["rows"][1]["DMIxADX"] = float("nan")
    else:
        value["result"]["rows"][0].pop("ABSMA")
    emit(page, post, value)
    pane = page.panes[0]
    assert pane._dmi_state == "unknown"
    assert all(v is None for v in pane.si_plots[0].lines["DMIxADX"])


def test_old_request_after_data_change_is_ignored(page):
    old = enable(page)
    raw(page, rows(31))
    new = page.client.calls[-1]
    emit(page, old)
    assert page.panes[0]._dmi_tag == new[3]
    assert page.panes[0]._dmi_state == "queued"


def test_core_epoch_needs_fresh_candles_before_new_post(page):
    old = enable(page)
    page.update_state({"core": {"epoch": "b", "capabilities": {"chart_series": True}}})
    assert page.panes[0]._dmi_state == "awaiting_data"
    emit(page, old)
    raw(page)
    new = page.client.calls[-1]
    assert new[0] == "POST" and new[2]["id"] != old[2]["id"]


def test_expired_values_clear_and_explicit_retry_uses_new_id(page):
    post = enable(page)
    emit(page, post, response(post, "expired"))
    pane = page.panes[0]
    assert not pane._dmi_lines and pane.producer_retry.isVisible()
    pane.producer_retry.click()
    assert page.client.calls[-1][2]["id"] != post[2]["id"]


def test_disconnected_or_closed_page_does_not_apply_late_complete(page):
    post = enable(page)
    page.client.attached = False
    page.client.connectionChanged.emit(False)
    emit(page, post)
    assert not page.panes[0]._dmi_lines
    page.client.attached = True
    page.close()
    emit(page, post)
    assert not page.panes[0]._dmi_lines


def test_same_epoch_reconnect_checks_original_job_and_language_is_display_only(page):
    post = enable(page)
    page.client.attached = False
    page.client.connectionChanged.emit(False)
    page.client.attached = True
    page.client.connectionChanged.emit(True)
    pane = page.panes[0]
    pane._poll_dmi()
    get = page.client.calls[-1]
    assert get[0:2] == ("GET", "/api/chart-series/" + post[2]["id"])
    emit(page, get, response(post))
    page.client.calls.clear()
    page.set_presentation("light", "ko")
    assert "당시 인지" in pane.producer_status.text()
    assert not page.client.calls


@pytest.mark.parametrize("width", [800, 1360])
def test_dmi_settings_and_series_layout(page, app, tmp_path, width):
    post = enable(page)
    emit(page, post)
    page.resize(width, 900)
    page.panes[0].settings_panel.set_draft(page.panes[0].profile)
    page.panes[0].settings_button.setChecked(True)
    app.processEvents()
    assert page.grab().save(str(tmp_path / f"chart-dmi-{width}.png"))
