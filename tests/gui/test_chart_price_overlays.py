"""Price overlays use exact core-produced bars and selected-close confirmations."""

import importlib.util
import json
import os
from copy import deepcopy
from dataclasses import asdict
from hashlib import sha256

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["chart-price-overlays-tests"])


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


def raw(page, *, source="database", count=30):
    from omega.gui.models import Candle

    page.accept_candles(
        page.panes[0].selection.text(),
        {
            "candles": [asdict(Candle(i * 60000, 10, 14, 8, 12, 5)) for i in range(count)],
            "source": source,
        },
    )


@pytest.fixture
def page(app, tmp_path):
    from omega.gui.charts_page import ChartsPage

    widget = ChartsPage(client(), root=tmp_path)
    widget.resize(1360, 900)
    widget.show()
    widget.set_presentation("dark", "en")
    widget.update_state({"core": {"epoch": "a", "capabilities": {"chart_series": True}}})
    raw(widget)
    app.processEvents()
    widget.client.calls.clear()
    yield widget
    widget.close()
    widget.deleteLater()
    app.processEvents()


def enable(page, kind):
    pane = page.panes[0]
    draft = deepcopy(pane.profile)
    draft[kind.lower()]["enabled"] = True
    pane.apply_profile(draft)
    return page.client.calls[-1]


def response(post, *, state="complete", epoch="a"):
    params = post[2]["parameters"]
    width = params["period_minutes"] * 60000
    produced = params["end_ms"] + 2000
    rows = []
    for opening in range(
        (params["start_ms"] + width - 1) // width * width, params["end_ms"] - width + 1, width
    ):
        row = {
            "open_time_ms": opening,
            "close_time_ms": opening + width,
            "available_at_ms": produced,
            "segment": 0,
        }
        if params["kind"] == "PSAR":
            row.update(PSAR=None if opening == 0 else 11.0, DCC=None if opening == 0 else 0)
        elif opening < width:
            row.update(LSTIMESTAMP=None, LSPRICE=None, LSTYPE=None, CONFIRMED_AT_MS=None)
        else:
            pivot = max(0, (opening // width - 1) // 3 * 3) * width
            row.update(
                LSTIMESTAMP=pivot // 1000,
                LSPRICE=9.0 + (pivot // width % 2),
                LSTYPE=-1 if pivot // width % 2 == 0 else 1,
                CONFIRMED_AT_MS=pivot + width * 2,
            )
        rows.append(row)
    config = {
        key: val
        for key, val in params.items()
        if key not in {"instrument", "period_minutes", "start_ms", "end_ms", "kind"}
    }
    result = {
        "instrument": params["instrument"],
        "kind": params["kind"],
        "period_minutes": params["period_minutes"],
        **config,
        "source": "database",
        "as_of_ms": produced - 1000,
        "produced_at_ms": produced,
        "source_checksum": "c" * 64,
        "knowledge_mode": "retrospective_display",
        "quality": {},
        "rows": rows,
    }
    return {
        "id": post[2]["id"],
        "parameters": params,
        "state": state,
        "spec_hash": sha256(
            json.dumps(params, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "core_epoch": epoch,
        "result": result if state == "complete" else None,
    }


def emit(page, post, result=None):
    page.client.resultReceived.emit(post[3], response(post) if result is None else result)


@pytest.mark.parametrize("kind", ["PSAR", "SWING"])
def test_common_api_exact_mapping_and_display_only_changes_make_no_requests(page, kind):
    post = enable(page, kind)
    pane = page.panes[0]
    assert post[0:2] == ("POST", "/api/chart-series")
    assert post[2]["parameters"]["end_ms"] == 30 * 60000
    emit(page, post)
    request = pane.price_requests[kind]
    assert request.state == "complete"
    assert len(request.rows) == len(pane.chart.candles)
    accepted = request.rows
    before = len(page.client.calls)
    draft = deepcopy(pane.profile)
    draft[kind.lower()]["color"] = "#cceeff"
    draft[kind.lower()]["width"] = 2.5
    draft["timezone"] = "UTC+09:00"
    pane.apply_profile(draft)
    assert len(page.client.calls) == before
    assert request.rows is accepted
    assert pane.chart.price_overlays[kind] == (accepted, "#cceeff", 2.5)
    assert len(pane.si_plots) == 3


def test_swing_selection_respects_confirmation_and_does_not_bridge_gap(page):
    post = enable(page, "SWING")
    emit(page, post)
    chart = page.panes[0].chart
    assert chart.confirmed_swing_segments() == ()
    chart.selected_timestamp = 2 * 60000
    assert chart.confirmed_swing_segments() == ()  # Only the first pivot is known.
    chart.selected_timestamp = 4 * 60000
    assert len(chart.confirmed_swing_segments()) == 1
    cached = chart.confirmed_swing_segments()
    assert chart.confirmed_swing_segments() is cached
    chart.selected_timestamp = 7 * 60000
    assert len(chart.confirmed_swing_segments()) == 2
    chart.clear_selection()
    assert chart.confirmed_swing_segments() == ()
    chart.set_price_overlay(
        "SWING",
        (
            (60000, 120000, 0, 9.0, -1, 120000, 0),
            (240000, 300000, 180000, 12.0, 1, 300000, 1),
            (420000, 480000, 360000, 9.0, -1, 480000, 1),
        ),
        color="#cceeff",
        width=1.5,
    )
    chart.selected_timestamp = 7 * 60000
    segments = chart.confirmed_swing_segments()
    assert len(segments) == 1
    assert segments[0][0] == (210000, 12.0)


@pytest.mark.parametrize("kind", ["PSAR", "SWING"])
def test_period_and_numeric_changes_new_request_old_response_ignored(page, kind):
    first = enable(page, kind)
    pane = page.panes[0]
    pane.set_timeframe(5)
    second = page.client.calls[-1]
    assert second[2]["parameters"]["period_minutes"] == 5
    assert second[2]["id"] != first[2]["id"]
    emit(page, first)
    assert pane.price_requests[kind].rows == ()
    emit(page, second)
    assert len(pane.price_requests[kind].rows) == 6
    draft = deepcopy(pane.profile)
    draft[kind.lower()]["start" if kind == "PSAR" else "swing_range"] = 0.03
    pane.apply_profile(draft)
    assert page.client.calls[-1][2]["id"] != second[2]["id"]


def test_unknown_post_only_looks_up_same_id_and_expiry_requires_explicit_retry(page):
    post = enable(page, "PSAR")
    request = page.panes[0].price_requests["PSAR"]
    page.client.failed.emit(post[3], "lost response")
    request.poll()
    lookup = page.client.calls[-1]
    assert lookup[0:2] == ("GET", "/api/chart-series/" + post[2]["id"])
    expired = response(post, state="expired")
    page.client.resultReceived.emit(lookup[3], expired)
    assert request.rows == () and request.state == "expired"
    assert request.retry.isVisible()
    before = len(page.client.calls)
    request.poll()
    assert len(page.client.calls) == before
    request.retry.click()
    assert page.client.calls[-1][0] == "POST"
    assert page.client.calls[-1][2]["id"] != post[2]["id"]


@pytest.mark.parametrize("fault", ["future_pivot", "knowledge", "gap", "hash", "params_bool"])
def test_malformed_or_future_response_is_not_plotted(page, fault):
    post = enable(page, "SWING")
    body = response(post)
    if fault == "future_pivot":
        body["result"]["rows"][1]["CONFIRMED_AT_MS"] = 999999999
    elif fault == "knowledge":
        body["result"]["rows"][1]["available_at_ms"] = 120000
    elif fault == "gap":
        body["result"]["rows"].pop(2)  # Same segment cannot skip a closed target bar.
    elif fault == "hash":
        body["spec_hash"] = "b" * 64
    else:
        body["parameters"] = {**body["parameters"], "period_minutes": True}
    emit(page, post, body)
    request = page.panes[0].price_requests["SWING"]
    assert request.state == "unknown" and request.rows == ()
    assert page.panes[0].chart.price_overlays["SWING"][0] == ()


def test_old_epoch_disconnect_and_demo_do_not_mix_database_producer(page):
    post = enable(page, "PSAR")
    emit(page, post, response(post, epoch="old-core"))
    assert page.panes[0].price_requests["PSAR"].rows == ()
    page.client.attached = False
    page.client.connectionChanged.emit(False)
    emit(page, post)
    assert page.panes[0].price_requests["PSAR"].rows == ()
    page.client.attached = True
    raw(page, source="demo")
    request = page.panes[0].price_requests["PSAR"]
    assert request.state == "source_unavailable"
    assert "no demo mixing" in request.label.text()


def test_v2_in_memory_migration_preserves_file_and_old_defaults(page, app):
    from omega.gui.chart_profiles import validate_profile
    from omega.gui.charts_page import ChartsPage

    old = deepcopy(page.panes[0].profile)
    old["schema_version"] = 2
    old.pop("psar")
    old.pop("swing")
    old.pop("auxiliary")
    old["enabled"] = {code: old["enabled"][code] for code in ("VOL", "MFI", "MMACD", "DMIxADX")}
    before = deepcopy(old)
    migrated = validate_profile(old)
    assert old == before and migrated["schema_version"] == 6
    assert not migrated["psar"]["enabled"] and not migrated["swing"]["enabled"]
    assert migrated["slots"] == old["slots"]
    document = page.workspace_snapshot()
    document["panes"][0]["profile"] = old
    page.store.path.parent.mkdir(parents=True, exist_ok=True)
    page.store.path.write_text(json.dumps(document), encoding="utf-8")
    encoded = page.store.path.read_bytes()
    reopened = ChartsPage(client(), root=page.store.path.parent.parent)
    try:
        assert reopened.panes[0].profile == migrated
        assert page.store.path.read_bytes() == encoded
    finally:
        reopened.close()
        reopened.deleteLater()
        app.processEvents()


@pytest.mark.parametrize("width", [800, 1360])
def test_structured_overlays_apply_and_remain_accessible(page, app, width):
    page.resize(width, 900)
    pane = page.panes[0]
    pane.settings_button.setChecked(True)
    editor = pane.settings_panel
    editor.tabs.setCurrentIndex(2)
    editor.overlay_controls[("psar", "enabled")].setChecked(True)
    editor.overlay_controls[("psar", "acceleration")].setText("0.01")
    assert editor.apply()
    post = page.client.calls[-1]
    assert post[2]["parameters"]["acceleration"] == 0.01
    emit(page, post)
    app.processEvents()
    assert editor.overlay_controls[("psar", "enabled")].isVisible()
    assert editor.overlay_controls[("psar", "acceleration")].width() >= 100
    assert page.width() == width
    assert not page.grab().isNull()
    assert pane.price_requests["PSAR"].state == "complete"


@pytest.mark.parametrize(
    "group,field",
    [("psar", "start"), ("psar", "maximum"), ("psar", "width"), ("swing", "swing_range")],
)
def test_profile_giant_json_integer_is_a_validation_error(group, field):
    from omega.gui.chart_profiles import default_profile, validate_profile

    profile = default_profile()
    profile[group][field] = 10**1000
    with pytest.raises(ValueError):
        validate_profile(profile)
