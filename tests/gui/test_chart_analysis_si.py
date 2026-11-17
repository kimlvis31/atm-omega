"""New analysis SI protocol/settings rendering; fixtures are not native market evidence."""

import importlib.util
import json
import os
from copy import deepcopy
from dataclasses import asdict
from hashlib import sha256

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")
MINUTE = 60000


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["chart-analysis-si-tests"])


@pytest.fixture
def page(app, tmp_path):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.charts_page import ChartsPage

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

    widget = ChartsPage(Client(), root=tmp_path)
    widget.resize(1360, 950)
    widget.show()
    state(widget)
    raw(widget)
    widget.set_presentation("light", "en")
    widget.client.calls.clear()
    yield widget
    widget.close()
    widget.deleteLater()
    app.processEvents()


def state(page, epoch="a", kinds=("WOI", "MST", "STS")):
    page.update_state(
        {
            "core": {
                "epoch": epoch,
                "capabilities": {
                    "chart_series": True,
                    "chart_analysis_kinds": list(kinds),
                },
            }
        }
    )


def raw(page, begin=200, count=30):
    from omega.gui.models import Candle

    page.accept_candles(
        page.panes[0].selection.text(),
        {
            "source": "database",
            "candles": [
                asdict(Candle(i * MINUTE, 100, 104, 98, 102, 5))
                for i in range(begin, begin + count)
            ],
        },
    )


def submissions(page):
    return [call for call in page.client.calls if call[:2] == ("POST", "/api/chart-series")]


def enable(page, kind, lines=1):
    editor = page.panes[0].settings_panel
    editor.enabled[kind].setChecked(True)
    editor.allocations[1].setCurrentText(kind)
    editor.displays[1].setChecked(True)
    if kind != "STS":
        for line in editor.analysis_controls[kind]["lines"][:lines]:
            line["active"].setChecked(True)
    assert editor.apply()
    return submissions(page)[-1] if submissions(page) else None


def reply(post, *, missing=None):
    from omega.domain.chart_analysis import ANALYSIS_FIELDS

    params = post[2]["parameters"]
    kind, period = params["kind"], params["period_minutes"]
    line_ids = (
        ("0",)
        if kind == "STS"
        else tuple(str(line["line_index"]) for line in params["definitions"])
    )
    produced = params["end_ms"] + 2000
    rows = []
    for index, opening in enumerate(range(params["start_ms"], params["end_ms"], period * MINUTE)):
        values, quality = {}, {}
        for line in line_ids:
            values[line] = {
                field: None
                if index == 0 or opening == missing
                else (3.0 if kind == "WOI" and field != "ABSMAREL" else 0.1 * (column + 1))
                for column, field in enumerate(ANALYSIS_FIELDS[kind])
            }
            quality[line] = (
                {
                    "ready": opening != missing,
                    "metric_reason": "missing" if opening == missing else "carried",
                    "metric_age_ms": None if opening == missing else 1000,
                    "trade_flow_reason": "observed",
                    "partial_prefix": False,
                }
                if kind == "MST"
                else {
                    "ready": opening != missing,
                    "channel": "depth",
                    "reason": "missing" if opening == missing else "observed",
                }
            )
        rows.append(
            {
                "open_time_ms": opening,
                "close_time_ms": opening + period * MINUTE,
                "segment": 0,
                "available_at_ms": produced,
                "values": values,
                "input_quality": quality,
                "source_available_at_ms": None
                if opening == missing
                else opening + period * MINUTE + 1,
                "auxiliary_checksum": "b" * 64,
            }
        )
    requested = (params["end_ms"] - params["start_ms"]) // MINUTE
    result = {
        **{key: value for key, value in params.items() if key not in {"start_ms", "end_ms"}},
        "series_schema_version": 1,
        "source": "database",
        "auxiliary_sources": sorted(
            params["instrument"] + suffix
            for suffix in (
                (":aggTrade", ":open_interest_history", ":long_short_ratio")
                if kind == "MST"
                else (":binance_depth_ws_v1",)
            )
        ),
        "source_checksum_scope": "committed_market_frames_native_revisions_v1",
        "replay_start_ms": params["start_ms"],
        "as_of_ms": produced - 1000,
        "produced_at_ms": produced,
        "knowledge_mode": "retrospective_display",
        "source_checksum": "c" * 64,
        "rows": rows,
        "quality": {
            "requested_minutes": requested,
            "observed_minutes": requested,
            "missing_minutes": 0,
            "reset_count": 0,
            "complete_bars": len(rows),
        },
    }
    return {
        "id": post[2]["id"],
        "parameters": params,
        "state": "complete",
        "core_epoch": "a",
        "spec_hash": sha256(
            json.dumps(params, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "result": result,
    }


def deliver(page, post, data=None):
    page.client.resultReceived.emit(post[3], reply(post) if data is None else data)


def test_legacy_profile_is_exact_until_a_real_new_family_edit(page):
    from omega.gui.chart_profiles import default_profile

    pane, editor = page.panes[0], page.panes[0].settings_panel
    original = deepcopy(default_profile())
    encoded = json.dumps(original, sort_keys=True)
    editor.set_draft(original)
    editor.set_presentation()
    pane._refresh_indicators()
    assert json.dumps(original, sort_keys=True) == encoded
    assert pane.profile == editor.draft() == original
    assert not any(kind.lower() in pane.profile for kind in ("WOI", "MST", "STS"))
    assert not any(kind in pane.profile["enabled"] for kind in ("WOI", "MST", "STS"))
    assert not page.store.path.exists() and not submissions(page)
    editor.analysis_controls["STS"]["WWOI"].setText("0.125")
    assert pane.profile == original
    promoted = editor.draft()
    assert promoted["schema_version"] == 7 and promoted["sts"]["WWOI"] == 0.125
    assert promoted["nes"] == original["nes"] and promoted["tpd"] == original["tpd"]
    assert not any(promoted["enabled"][kind] for kind in ("WOI", "MST", "STS"))


def test_explicit_family_assignment_promotes_choices_without_enabling_calculation(page):
    from omega.gui.chart_profiles import INDICATORS, LEGACY_INDICATORS

    pane, editor = page.panes[0], page.panes[0].settings_panel
    combo = editor.allocations[0]
    assert tuple(combo.itemText(index) for index in range(combo.count())) == LEGACY_INDICATORS
    editor.analysis_allocation_slots["MST"].setCurrentIndex(2)
    editor._assign_analysis("MST")
    assert tuple(combo.itemText(index) for index in range(combo.count())) == INDICATORS
    assert editor.allocations[2].currentText() == "MST"
    assert pane.profile["schema_version"] == 6 and not editor.enabled["MST"].isChecked()
    assert editor.apply()
    assert pane.profile["schema_version"] == 7 and pane.profile["slots"][2]["indicator"] == "MST"
    assert not pane.profile["enabled"]["MST"] and not submissions(page)


@pytest.mark.parametrize("kind", ["WOI", "MST", "STS"])
def test_new_family_apply_save_roundtrip_and_three_slot_swap(page, kind, app):
    from omega.gui.charts_page import ChartsPage

    post = enable(page, kind, 2)
    assert post and len(page.panes[0].si_plots) == 3
    editor = page.panes[0].settings_panel
    editor.allocations[0].setCurrentText(kind)
    assert [combo.currentText() for combo in editor.allocations][:2] == [kind, "VOL"]
    assert editor.displays[0].isChecked() and editor.displays[1].isChecked()
    assert editor.apply() and not page.store.path.exists()
    editor.profile_name.setText("analysis")
    editor.save()
    saved = page.store.load()["profiles"]["analysis"]
    assert saved == page.panes[0].profile and saved["schema_version"] == 7
    reopened = ChartsPage(page.client, root=page.store.path.parent.parent)
    assert reopened.panes[0].profile == saved
    reopened.close()
    reopened.deleteLater()
    app.processEvents()


@pytest.mark.parametrize("kind", ["WOI", "MST"])
def test_family_batch_includes_hidden_active_lines_and_draws_all_ten(page, kind, app):
    post = enable(page, kind, 10)
    assert len(post[2]["parameters"]["definitions"]) == 10
    deliver(page, post)
    pane, plot = page.panes[0], page.panes[0].si_plots[1]
    assert pane.derived_requests[kind].state == "complete" and len(plot.lines) == 10
    app.processEvents()
    plot._geometry()
    assert len(plot._paths) == 10 and all(not path.isEmpty() for _, path in plot._paths)
    assert len(plot.line_styles) == 10
    before = len(submissions(page))
    editor = pane.settings_panel
    editor.analysis_controls[kind]["lines"][9]["visible"].setChecked(False)
    editor.analysis_controls[kind]["lines"][8]["color"].setText("#123456")
    editor.analysis_controls[kind]["lines"][8]["width"].setText("2.5")
    editor.analysis_controls[kind]["display"].setCurrentIndex(1)
    editor.zone.setCurrentText("UTC+09:00")
    assert editor.apply()
    assert len(submissions(page)) == before and len(plot.lines) == 9
    assert len(pane.derived_requests[kind].params["definitions"]) == 10
    assert any(
        style["color"] == "#123456" and style["width"] == 2.5 for style in plot.line_styles.values()
    )
    app.processEvents()
    plot._geometry()
    paths = plot._paths
    pane.chart.set_cursor(210 * MINUTE)
    app.processEvents()
    assert plot._paths is paths


def test_no_active_lines_old_core_and_numerical_edit_have_explicit_states(page):
    editor = page.panes[0].settings_panel
    editor.enabled["WOI"].setChecked(True)
    assert editor.apply()
    request = page.panes[0].derived_requests["WOI"]
    assert request.state == "no_active_lines" and not submissions(page)
    state(page, kinds=())
    editor.analysis_controls["WOI"]["lines"][0]["active"].setChecked(True)
    assert editor.apply()
    assert request.state == "kind_unavailable" and not submissions(page)
    state(page)
    assert len(submissions(page)) == 1
    first = submissions(page)[0]
    editor.analysis_controls["WOI"]["lines"][0]["nSamples"].setText("23")
    assert editor.apply()
    assert len(submissions(page)) == 2
    assert submissions(page)[-1][2]["parameters"]["definitions"] == [
        {"line_index": 0, "nSamples": 23}
    ]
    assert any(
        call[1] == "/api/chart-series/" + first[2]["id"] + "/cancel" for call in page.client.calls
    )


@pytest.mark.parametrize("kind", ["WOI", "MST", "STS"])
@pytest.mark.parametrize("change", ["epoch", "origin", "instrument", "disconnect", "off"])
def test_old_reply_cannot_cross_context_or_restore_values(page, kind, change):
    post = enable(page, kind)
    pane, editor = page.panes[0], page.panes[0].settings_panel
    if change == "epoch":
        state(page, epoch="b")
    elif change == "origin":
        editor.analysis_controls[kind]["origin_ms"].setText(str(103 * MINUTE))
        assert editor.apply()
    elif change == "instrument":
        pane.selection.setText("binance:usdm:ETHUSDT")
    elif change == "off":
        editor.enabled[kind].setChecked(False)
        assert editor.apply()
    else:
        page.client.attached = False
        page.client.connectionChanged.emit(False)
    deliver(page, post)
    assert pane.derived_requests[kind].rows == ()
    assert all(value is None for line in pane.si_plots[1].lines.values() for value in line)


@pytest.mark.parametrize("kind", ["WOI", "MST", "STS"])
@pytest.mark.parametrize(
    "change", ["held", "field", "quality", "availability", "checksum", "source", "nan"]
)
def test_malformed_native_result_is_rejected_without_flat_or_held_values(page, kind, change):
    post = enable(page, kind)
    data = reply(post)
    row = data["result"]["rows"][5]
    if change == "held":
        row["input_quality"]["0"]["ready"] = False
        if kind == "MST":
            row["input_quality"]["0"]["metric_reason"] = "missing"
        else:
            row["input_quality"]["0"]["reason"] = "missing"
    elif change == "field":
        row["values"]["0"]["invented"] = 0
    elif change == "quality":
        row["input_quality"]["0"]["ready"] = 1
    elif change == "availability":
        row["source_available_at_ms"] = data["result"]["as_of_ms"] + 1
    elif change == "checksum":
        row["auxiliary_checksum"] = "0" * 64
    elif change == "source":
        data["result"]["auxiliary_sources"] = ["different"]
    else:
        row["values"]["0"][next(iter(row["values"]["0"]))] = float("nan")
    deliver(page, post, data)
    request = page.panes[0].derived_requests[kind]
    assert request.state == "unknown" and request.rows == ()


@pytest.mark.parametrize("kind", ["WOI", "MST", "STS"])
def test_pinned_timestamp_retains_native_missing_reason_and_display_switch_uses_cache(page, kind):
    post = enable(page, kind)
    deliver(page, post, reply(post, missing=205 * MINUTE))
    pane = page.panes[0]
    pane.chart.selected_timestamp = 205 * MINUTE
    pane.chart.selectionChanged.emit(205 * MINUTE)
    first = pane.selected_readout.text()
    analysis_readout = first.split(kind + " ", 1)[1]
    assert "missing" in analysis_readout and "warmup" not in analysis_readout
    pane.chart.set_cursor(210 * MINUTE)
    assert pane.chart.selected_timestamp == 205 * MINUTE and pane.selected_readout.text() == first
    before = len(submissions(page))
    pane.settings_panel.analysis_controls[kind]["display"].setCurrentIndex(1)
    assert pane.settings_panel.apply()
    assert len(submissions(page)) == before and "missing" in pane.selected_readout.text()


def test_scalar_uses_signed_histogram_all_components_and_zero_is_real(page, app):
    post = enable(page, "STS")
    data = reply(post)
    for row in data["result"]["rows"]:
        row["values"]["0"].update(S=0.5 if row["open_time_ms"] % (2 * MINUTE) else -0.25, D=0.0)
    deliver(page, post, data)
    pane, plot = page.panes[0], page.panes[0].si_plots[1]
    app.processEvents()
    plot._geometry()
    assert plot.histogram and any(
        not path.isEmpty() for name, path in plot._paths if ":True:" in name
    )
    assert any(not path.isEmpty() for name, path in plot._paths if ":False:" in name)
    before = len(submissions(page))
    combo = pane.settings_panel.analysis_controls["STS"]["display"]
    for field in ("S", "M", "F", "D", "W"):
        combo.setCurrentIndex(combo.findData(field))
        assert pane.settings_panel.apply()
        assert tuple(plot.lines) == ("STS · " + field,)
    combo.setCurrentIndex(combo.findData("D"))
    assert pane.settings_panel.apply()
    assert all(value == 0 for value in next(iter(plot.lines.values())))
    assert len(submissions(page)) == before


def test_fixed_origin_does_not_slide_and_daily_default_includes_seed_window(page):
    post = enable(page, "WOI")
    assert post[2]["parameters"]["start_ms"] == 102 * MINUTE
    raw(page, count=31)
    assert submissions(page)[-1][2]["parameters"]["start_ms"] == 102 * MINUTE
    pane = page.panes[0]
    pane.minutes = 1440
    raw(page, begin=199 * 1440, count=1440)
    params = submissions(page)[-1][2]["parameters"]
    assert params["period_minutes"] == 1440
    assert params["start_ms"] == 72 * 1440 * MINUTE
    assert params["end_ms"] - params["start_ms"] == 128 * 1440 * MINUTE
    assert "UTC" in pane.derived_requests["WOI"].label.text()


@pytest.mark.parametrize("kind", ["WOI", "MST", "STS"])
def test_cancel_progress_and_actionable_deadline_failure_are_truthful(page, kind):
    post = enable(page, kind)
    data = reply(post)
    data.update(state="cancelling", result=None)
    deliver(page, post, data)
    request = page.panes[0].derived_requests[kind]
    assert request.state == "cancelling" and request.rows == () and request.timer.isActive()
    request.poll()
    lookup = page.client.calls[-1]
    assert lookup[:2] == ("GET", "/api/chart-series/" + post[2]["id"])
    data.update(state="failed", error="compute deadline reached; choose a smaller replay window")
    page.client.resultReceived.emit(lookup[3], data)
    assert request.state == "failed" and "smaller replay window" in request.label.text()
    assert request.retry.isVisible()


@pytest.mark.parametrize(
    "change",
    [
        "old_newkey",
        "bool_sample",
        "ten_lines",
        "negative_weight",
        "zero_weights",
        "bad_color",
        "nan_width",
    ],
)
def test_new_profile_strict_validation_preserves_legacy_contract(change):
    from omega.gui.chart_profiles import default_profile, promote_analysis_profile, validate_profile

    profile = promote_analysis_profile(default_profile())
    if change == "old_newkey":
        profile["schema_version"] = 6
    elif change == "bool_sample":
        profile["woi"]["lines"][0]["nSamples"] = True
    elif change == "ten_lines":
        profile["mst"]["lines"].pop()
    elif change == "negative_weight":
        profile["sts"]["WWOI"] = -0.1
    elif change == "zero_weights":
        for key in ("WMMACD", "WMFI", "WDMIxADX", "WWOI"):
            profile["sts"][key] = 0
    elif change == "bad_color":
        profile["woi"]["lines"][0]["color"] = "red"
    else:
        profile["mst"]["lines"][0]["width"] = float("nan")
    with pytest.raises(ValueError):
        validate_profile(profile)
