"""Exact-wire MAIN window context, compact drafts and signed presentation paths."""

import importlib.util
import json
import math
import os
from copy import deepcopy
from hashlib import sha256
from unittest.mock import Mock

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["chart-price-windows-tests"])


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
    widget.resize(1360, 900)
    widget.show()
    widget.update_state({"core": {"epoch": "a", "capabilities": {"chart_series": True}}})
    accept(widget)
    app.processEvents()
    widget.client.calls.clear()
    yield widget
    widget.close()
    widget.deleteLater()
    app.processEvents()


def wire(count=30, *, close="100.000000000000000001", start=0):
    return [
        dict(
            open_time_ms=(start + index) * 60000,
            open="100",
            high="104",
            low="98",
            close=close,
            volume="5",
        )
        for index in range(count)
    ]


def accept(page, rows=None, source="database"):
    page.accept_candles(
        page.panes[0].selection.text(),
        {
            "source": source,
            "candles": rows if rows is not None else wire(),
            "core_epoch": page._series_epoch,
        },
    )


def enable(page, *, code="bol", index=0, width=-2):
    pane = page.panes[0]
    profile = deepcopy(pane.profile)
    profile["price_windows"][code]["enabled"] = True
    profile["price_windows"][code]["lines"][index]["active"] = True
    profile["price_windows"][code]["lines"][index]["nSamples"] = 2
    if code == "bol":
        profile["price_windows"][code]["lines"][index]["bandWidth"] = width
    pane.apply_profile(profile)
    return page.client.calls[-1] if page.client.calls else None


def reply(page, post):
    from omega.gui.chart_price_windows import output_columns

    spec = post[2]["parameters"]
    produced = spec["end_ms"] + 2000
    width = spec["period_minutes"] * 60000
    rows = []
    for opening in range(
        (spec["display_start_ms"] + width - 1) // width * width, spec["end_ms"] - width + 1, width
    ):
        values = []
        for item in spec["definitions"]:
            values.extend([103.0, 97.0, 100.0] if item["analysis_code"] == "BOL" else [0.0])
        rows.append(
            dict(
                open_time_ms=opening,
                close_time_ms=opening + width,
                available_at_ms=produced,
                segment=0,
                values=values,
                ready=[True] * len(spec["definitions"]),
            )
        )
    minutes = (spec["end_ms"] - spec["start_ms"]) // 60000
    result = {
        "instrument": spec["instrument"],
        "kind": "PRICE_WINDOWS",
        "period_minutes": spec["period_minutes"],
        "definitions": spec["definitions"],
        "replay_start_ms": spec["start_ms"],
        "display_start_ms": spec["display_start_ms"],
        "end_ms": spec["end_ms"],
        "source": "database",
        "as_of_ms": produced - 1000,
        "produced_at_ms": produced,
        "knowledge_mode": "retrospective_display",
        "source_checksum_scope": "omega_chart_candle_content_v1",
        "source_checksum": "c" * 64,
        "display_content_pin": page.panes[0]._price_content[2],
        "columns": output_columns(spec["definitions"], spec["period_minutes"]),
        "rows": rows,
        "quality": dict(
            requested_minutes=minutes,
            observed_minutes=minutes,
            missing_minutes=0,
            reset_count=0,
            complete_bars=len(rows),
        ),
    }
    return {
        "id": post[2]["id"],
        "parameters": deepcopy(spec),
        "state": "complete",
        "core_epoch": "a",
        "spec_hash": sha256(
            json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "result": result,
    }


@pytest.mark.parametrize("version", [1, 2, 3, 4, 5])
def test_price_main_v5_migration_is_memory_only(app, tmp_path, version):
    from omega.gui.chart_profiles import ChartWorkspaceStore, default_profile, validate_profile

    old = default_profile()
    old.pop("price_windows")
    old["schema_version"] = version
    before = deepcopy(old)
    store = ChartWorkspaceStore(tmp_path)
    store.path.parent.mkdir(parents=True)
    document = {
        "schema_version": 1,
        "profiles": {"old": old},
        "panes": [{"instrument": "binance:spot:BTCUSDT", "timeframe": 1, "profile": old}],
    }
    encoded = json.dumps(document).encode()
    store.path.write_bytes(encoded)
    loaded = store.load()
    current = validate_profile(old)
    assert old == before and current["schema_version"] == 6
    assert loaded["schema_version"] == 1 and store.path.read_bytes() == encoded
    for code in ("sma", "wma", "bol"):
        assert current["price_windows"][code]["enabled"] is False
        assert len(current["price_windows"][code]["lines"]) == 10
        assert all(
            not line["active"] and line["visible"]
            for line in current["price_windows"][code]["lines"]
        )


def test_price_main_batch_and_presentation_only_zero_http(page):
    pane = page.panes[0]
    draft = deepcopy(pane.profile)
    for code in ("sma", "wma", "bol"):
        draft["price_windows"][code]["enabled"] = True
        for index in (0, 9):
            draft["price_windows"][code]["lines"][index].update(active=True, nSamples=2)
    pane.apply_profile(draft)
    assert len(page.client.calls) == 1
    post = page.client.calls[-1]
    items = post[2]["parameters"]["definitions"]
    assert [(item["analysis_code"], item["line_index"]) for item in items] == [
        ("BOL", 0),
        ("BOL", 9),
        ("SMA", 0),
        ("SMA", 9),
        ("WMA", 0),
        ("WMA", 9),
    ]
    page.client.resultReceived.emit(post[3], reply(page, post))
    request = pane.price_windows_request
    assert request.state == "complete"
    immutable = request.rows
    draft = deepcopy(pane.profile)
    draft["timezone"] = "UTC+09:00"
    draft["price_windows"]["bol"].update(show_center=False, show_band=False)
    for code in ("sma", "wma", "bol"):
        draft["price_windows"][code]["lines"][0].update(
            visible=False, color="#123456", opacity=111, stroke_width=3
        )
    pane.apply_profile(draft)
    assert len(page.client.calls) == 1 and request.rows is immutable
    assert pane.chart.price_window_snapshot[0] is immutable


def test_price_main_exact_wire_high_precision_changes_generation(page):
    pane = page.panes[0]
    post = enable(page)
    page.client.resultReceived.emit(post[3], reply(page, post))
    pin, display, generation = pane._price_content, pane.raw, pane._raw_revision
    accept(page, wire(close="100.000000000000000002"))
    assert pane.raw == display and pane._price_content != pin
    assert pane._raw_revision == generation + 1
    assert len(page.client.calls) == 2
    assert pane.price_windows_request.rows == ()
    second = page.client.calls[-1]
    page.client.resultReceived.emit(post[3], reply(page, post))
    assert pane.price_windows_request.state == "queued"
    page.client.resultReceived.emit(second[3], reply(page, second))
    assert pane.price_windows_request.state == "complete"
    count = len(page.client.calls)
    accept(page, wire(close="100.0000000000000000020"))
    assert len(page.client.calls) == count


def test_price_main_float_dto_has_no_exact_fallback(page):
    rows = wire()
    for row in rows:
        for name in ("open", "high", "low", "close", "volume"):
            row[name] = float(row[name])
    accept(page, rows)
    assert page.panes[0].raw and page.panes[0]._price_content is None
    assert enable(page) is None
    assert page.panes[0].price_windows_request.state == "source_unverifiable"


def test_price_main_pin_mismatch_is_terminal_without_resubmit(page):
    post = enable(page)
    response = reply(page, post)
    response["result"]["display_content_pin"] = "b" * 64
    page.client.resultReceived.emit(post[3], response)
    request = page.panes[0].price_windows_request
    assert request.state == "source_changed" and not request.rows
    request.poll()
    page.panes[0]._refresh_indicators()
    assert len(page.client.calls) == 1
    assert not page.panes[0].chart.price_window_snapshot[0]


@pytest.mark.parametrize(
    "field,value",
    [
        ("source", "demo"),
        ("source_checksum_scope", "different"),
        ("period_minutes", True),
        ("display_start_ms", True),
        ("produced_at_ms", False),
        ("unexpected", 1),
    ],
)
def test_price_main_strict_result_metadata(page, field, value):
    post = enable(page)
    response = reply(page, post)
    response["result"][field] = value
    page.client.resultReceived.emit(post[3], response)
    request = page.panes[0].price_windows_request
    assert request.state == "failed" and not request.rows
    request.poll()
    assert len(page.client.calls) == 1


@pytest.mark.parametrize(
    "mutate",
    [
        lambda row: row.update(available_at_ms=True),
        lambda row: row.update(segment=True),
        lambda row: row.update(ready=[1]),
        lambda row: row.update(ready=[False]),
        lambda row: row.update(values=[float("nan"), 1, 2]),
        lambda row: row.update(values=[True, 1, 2]),
        lambda row: row.update(values=[1, 2]),
    ],
)
def test_price_main_strict_rows_and_readiness(page, mutate):
    post = enable(page)
    response = reply(page, post)
    mutate(response["result"]["rows"][0])
    page.client.resultReceived.emit(post[3], response)
    assert page.panes[0].price_windows_request.state == "failed"


def test_price_main_center_independent_warmup_and_signed_readout(page):
    post = enable(page)
    response = reply(page, post)
    response["result"]["rows"][0].update(values=[None, None, 0.0], ready=[False])
    page.client.resultReceived.emit(post[3], response)
    request = page.panes[0].price_windows_request
    assert request.state == "complete" and request.rows[0][3] == (None, None, 0.0)
    assert "warmup" in request.readout(0)[0] and "MA=0" in request.readout(0)[0]
    label = request.readout(60000)[0]
    assert "BOLLOW=103" in label and "BOLHIGH=97" in label
    assert "bandWidth=-2" in label


def test_price_main_unknown_queries_same_id_expiry_requires_explicit_retry(page):
    post = enable(page)
    request = page.panes[0].price_windows_request
    page.client.failed.emit(post[3], "transport outcome unknown")
    request.poll()
    query = page.client.calls[-1]
    assert query[:2] == ("GET", "/api/chart-series/" + post[2]["id"])
    response = reply(page, post)
    response["state"] = "expired"
    page.client.resultReceived.emit(query[3], response)
    count = len(page.client.calls)
    request.poll()
    assert request.state == "expired" and len(page.client.calls) == count
    request.request_again()
    assert page.client.calls[-1][2]["id"] != post[2]["id"]


def test_price_main_epoch_disconnect_and_close_fences(page):
    post = enable(page)
    pane = page.panes[0]
    request = pane.price_windows_request
    page.client.attached = False
    page.client.connectionChanged.emit(False)
    assert not request.rows
    page.client.attached = True
    page.client.connectionChanged.emit(True)
    assert request.state == "source_unverifiable"
    count = len(page.client.calls)
    request.poll()
    assert len(page.client.calls) == count
    accept(page)
    second = page.client.calls[-1]
    assert second[0] == "POST" and second[2]["id"] != post[2]["id"]
    page.update_state({"core": {"epoch": "b", "capabilities": {"chart_series": True}}})
    page.client.resultReceived.emit(second[3], reply(page, second))
    assert not request.rows and request.state == "awaiting_data"
    page.close()
    assert not request.timer.isActive()


@pytest.mark.parametrize("body_epoch", ["a", None])
def test_price_main_pending_old_wire_body_cannot_become_fresh_epoch_b(page, body_epoch):
    pane = page.panes[0]
    first = enable(page)
    original = pane.raw
    # The connection URL and credentials stay attached. Only the core state
    # changes while an already pending GET is reading its candle provider.
    page.update_state({"core": {"epoch": "b", "capabilities": {"chart_series": True}}})
    body = {"source": "database", "candles": wire()}
    if body_epoch is not None:
        body["core_epoch"] = body_epoch
    page.accept_candles(pane.selection.text(), body)
    assert pane.raw == original and pane._raw_epoch == "b"
    assert pane._price_content is None and pane._price_raw_epoch is None
    request = pane.price_windows_request
    assert request.state == "source_unverifiable" and not request.rows
    forged = {
        "id": first[2]["id"],
        "parameters": first[2]["parameters"],
        "core_epoch": "b",
        "state": "complete",
        "result": {},
    }
    page.client.resultReceived.emit(first[3], forged)
    assert not request.rows
    assert sum(call[0] == "POST" for call in page.client.calls) == 1
    before = len(page.client.calls)
    request.poll()
    assert len(page.client.calls) == before
    assert "재조회" in request.label.text() and "재조회" in request.retry.text()
    request.request_again()
    assert page.client.calls[-1][0] == "GET"
    assert "include_core_epoch=1" in page.client.calls[-1][1]
    accept(page)
    current = page.client.calls[-1]
    assert current[0] == "POST" and current[2]["id"] != first[2]["id"]
    assert pane._price_raw_epoch == "b"
    response = reply(page, current)
    response["core_epoch"] = "b"
    page.client.resultReceived.emit(current[3], response)
    assert request.state == "complete" and request.rows
    # A second late unverified body conservatively invalidates even equal prices;
    # it must never silently preserve fresh B evidence for an unverified body.
    page.accept_candles(pane.selection.text(), body)
    assert pane.raw == original and pane._price_content is None
    assert request.state == "source_unverifiable" and not request.rows


def test_price_main_epoch_opt_in_only_for_active_new_definitions(page):
    from omega.gui.chart_price_windows import exact_snapshot_pin

    pane = page.panes[0]
    page.refresh()
    assert all("include_core_epoch" not in call[1] for call in page.client.calls)
    # An unflagged legacy body still displays normally, but grants no new pin.
    page.accept_candles(pane.selection.text(), {"source": "database", "candles": wire()})
    assert pane.raw and pane._price_content is None and pane._price_raw_epoch is None
    page.client.calls.clear()
    enable(page)
    request = pane.price_windows_request
    assert request.state == "source_unverifiable" and not page.client.calls
    request.request_again()
    assert len(page.client.calls) == 1 and "include_core_epoch=1" in page.client.calls[0][1]
    # Direct callers cannot attach a pin without explicit matching provenance.
    pin = exact_snapshot_pin(pane.selection.text(), wire())
    pane.set_snapshot(pane.raw, "database", {}, content=pin)
    assert pane._price_content is None
    pane.set_snapshot(pane.raw, "database", {}, content=pin, content_epoch="previous")
    assert pane._price_content is None
    pane.set_snapshot(pane.raw, "database", {}, content=pin, content_epoch="a")
    assert pane._price_content == pin and pane._price_raw_epoch == "a"


@pytest.mark.parametrize(
    "source,period,reason",
    [
        ("demo", 1, "source_unavailable"),
        ("database", 4320, "period_unavailable"),
        ("database", "1w", "period_unavailable"),
        ("database", "1M", "period_unavailable"),
    ],
)
def test_price_main_unsupported_no_substitute(page, source, period, reason):
    accept(page, source=source)
    page.panes[0].set_timeframe(period)
    page.client.calls.clear()
    enable(page)
    assert not page.client.calls and page.panes[0].price_windows_request.state == reason


def test_price_main_fixed_origin_does_not_slide_with_raw_window(page):
    first = enable(page)
    accept(page, wire(start=5))
    latest = page.client.calls[-1]
    assert latest[2]["parameters"]["start_ms"] == first[2]["parameters"]["start_ms"] == 0
    assert latest[2]["parameters"]["display_start_ms"] == 5 * 60000


def test_price_main_state_and_scalar_budget_fail_before_submission(page):
    pane = page.panes[0]
    before = deepcopy(pane.profile)
    profile = deepcopy(before)
    for code in ("sma", "wma", "bol"):
        profile["price_windows"][code]["enabled"] = True
        for line in profile["price_windows"][code]["lines"]:
            line.update(active=True, nSamples=20000)
    with pytest.raises(ValueError, match="state"):
        pane.apply_profile(profile)
    assert pane.profile == before and not page.client.calls
    accept(page, wire(count=500))
    for code in ("sma", "wma", "bol"):
        for line in profile["price_windows"][code]["lines"]:
            line["nSamples"] = 2
    pane.apply_profile(profile)
    assert pane.price_windows_request.state == "scalar_capacity"
    assert not page.client.calls


def test_price_main_missing_bar_breaks_segment_and_null_does_not_bridge(page):
    post = enable(page)
    response = reply(page, post)
    del response["result"]["rows"][1]
    response["result"]["quality"]["complete_bars"] -= 1
    # A gap without an explicit segment boundary must fail strict parsing.
    page.client.resultReceived.emit(post[3], response)
    request = page.panes[0].price_windows_request
    assert request.state == "failed" and not request.rows
    request.request_again()
    post = page.client.calls[-1]
    response = reply(page, post)
    del response["result"]["rows"][1]
    response["result"]["quality"]["complete_bars"] -= 1
    for row in response["result"]["rows"][1:]:
        row["segment"] = 1
    response["result"]["quality"]["reset_count"] = 1
    response["result"]["rows"][1].update(values=[None, None, None], ready=[False])
    page.client.resultReceived.emit(post[3], response)
    assert request.state == "complete"
    assert request.rows[1][2] == 1 and request.rows[1][3] == (None, None, None)


@pytest.mark.parametrize("width", [800, 1360])
def test_price_main_settings_in_existing_scroll_area_and_multiple_panes(page, app, width):
    page.resize(width, 900)
    page.add_chart("binance:spot:ETHUSDT")
    pane = page.panes[0]
    pane.settings_button.setChecked(True)
    pane.settings_panel.tabs.setCurrentIndex(2)
    app.processEvents()
    assert pane.settings_scroll.widget() is pane.settings_panel
    editor = pane.settings_panel.price_windows
    assert editor.index.count() == 10 and editor.family.count() == 3
    assert pane.settings_scroll.maximumHeight() == 275
    assert page.panes[1].price_windows_request is not pane.price_windows_request
    assert not any(call[0] == "POST" for call in page.client.calls)


def test_price_main_compact_editor_atomic_invalid_save(page):
    pane = page.panes[0]
    editor = pane.settings_panel.price_windows
    editor.family.setCurrentText("BOL")
    editor.index.setCurrentIndex(9)
    editor.master.setChecked(True)
    editor.active.setChecked(True)
    editor.ma_type.setCurrentText("WMA")
    editor.fields["bandWidth"].setText("-2")
    editor.fields["nSamples"].setText("2")
    assert pane.settings_panel.apply()
    assert pane.profile["price_windows"]["bol"]["lines"][9]["bandWidth"] == -2
    assert pane.profile["price_windows"]["bol"]["MAType"] == "WMA"
    before = deepcopy(pane.profile)
    pane.settings_panel.profile_name.setText("windows")
    pane.settings_panel.save()
    saved = page.store.path.read_bytes()
    editor.fields["bandWidth"].setText("NaN")
    assert pane.settings_panel.apply() is False
    pane.settings_panel.save()
    assert pane.profile == before and page.store.path.read_bytes() == saved
    editor.fields["bandWidth"].setText("0")
    editor.index.setCurrentIndex(0)
    editor.index.setCurrentIndex(9)
    assert editor.fields["bandWidth"].text() == "0"


def test_price_main_language_change_preserves_unsaved_invalid_indexed_draft(page):
    pane = page.panes[0]
    settings = pane.settings_panel
    profile = deepcopy(pane.profile)
    for index, line in enumerate(profile["price_windows"]["bol"]["lines"]):
        line["bandWidth"] = index - 5 if index % 2 else float(index - 5)
    settings.set_draft(profile)
    settings.profile_name.setText("localized")
    settings.save()
    saved = page.store.path.read_bytes()
    applied = deepcopy(pane.profile)
    native_widths = [
        (type(line["bandWidth"]), line["bandWidth"])
        for line in pane.profile["price_windows"]["bol"]["lines"]
    ]
    editor = settings.price_windows
    editor.family.setCurrentText("BOL")
    editor.index.setCurrentIndex(9)
    editor.master.setChecked(True)
    editor.active.setChecked(True)
    editor.fields["bandWidth"].setText("NaN")
    internal = deepcopy(editor._draft)
    selected = editor._selection
    request = pane.price_windows_request
    identity, parameters, rows = request.identity, deepcopy(request.params), request.rows
    calls = len(page.client.calls)
    for language in ("en", "ko", "en"):
        page.set_presentation("dark", language)
        assert editor._selection == selected and editor.index.currentIndex() == 9
        assert editor.family.currentText() == "BOL" and editor.ma_type.currentText() == "SMA"
        assert editor.fields["bandWidth"].text() == "NaN"
        assert editor.master.isChecked() and editor.active.isChecked()
        assert editor._draft == internal
        assert pane.profile == applied and page.store.path.read_bytes() == saved
        assert [
            (type(line["bandWidth"]), line["bandWidth"])
            for line in pane.profile["price_windows"]["bol"]["lines"]
        ] == native_widths
        assert len(page.client.calls) == calls
        assert (
            request.identity == identity and request.params == parameters and request.rows is rows
        )
        assert any(
            label.text() == ("표본 수" if language == "ko" else "Samples")
            for label, _korean, _english in editor.labels
        )
        assert ("UTC밀리초" if language == "ko" else "UTC milliseconds") in editor.note.text()
        assert ("빈칸" if language == "ko" else "Blank") in editor.origin.placeholderText()
        assert request.retry.text() == ("다시 계산" if language == "ko" else "Recalculate")
    assert settings.apply() is False
    assert pane.profile == applied and page.store.path.read_bytes() == saved
    assert len(page.client.calls) == calls


@pytest.mark.parametrize("band_width", [-2, 0, 2])
def test_price_main_signed_band_polygon_and_lod_gaps(page, band_width):
    from omega.gui.chart_price_windows import draw_price_windows

    post = enable(page, width=band_width)
    response = reply(page, post)
    endpoints = (
        [103.0, 97.0] if band_width < 0 else [100.0, 100.0] if band_width == 0 else [97.0, 103.0]
    )
    for row in response["result"]["rows"]:
        row["values"] = [*endpoints, 100.0]
    page.client.resultReceived.emit(post[3], response)
    chart = page.panes[0].chart
    chart._geometry()
    # This assertion checks named vertex order before clipping. Candle-only
    # automatic fit may exclude a band endpoint and rotate the clipped polygon.
    chart._price_range = (96.0, 104.0)

    class Painter:
        def __init__(self):
            self.paths, self.lines = [], []

        def save(self):
            pass

        def restore(self):
            pass

        def setClipRect(self, *_args):
            pass

        def setPen(self, *_args):
            pass

        def setBrush(self, *_args):
            pass

        def drawPath(self, path):
            self.paths.append(path)

        def drawLine(self, *points):
            self.lines.append(points)

    painter = Painter()
    draw_price_windows(chart, painter, bands=True)
    assert bool(painter.paths) is (band_width != 0)
    draw_price_windows(chart, painter, bands=False)
    assert len(painter.lines) == len(chart.candles) - 1
    if painter.paths:
        first, second = painter.paths[0].elementAt(0), painter.paths[0].elementAt(2)
        assert (first.y < second.y) is (band_width < 0)
        assert chart._plot().contains(painter.paths[0].boundingRect())
    # A retained LOD observation cannot connect across a hidden middle candle.
    chart._display = (chart.candles[0], chart.candles[2])
    painter = Painter()
    draw_price_windows(chart, painter, bands=False)
    assert not painter.lines


@pytest.mark.parametrize(
    "band_width,endpoints,viewport",
    [
        (-2, (103.0, 97.0), (98.0, 102.0)),
        (2, (97.0, 103.0), (98.0, 102.0)),
        (-2, (103.0, -5.0), (96.0, 104.0)),
        (2, (-5.0, 103.0), (96.0, 104.0)),
    ],
)
def test_price_main_clipped_band_geometry_preserves_named_input(
    page, band_width, endpoints, viewport
):
    from omega.gui.chart_price_windows import draw_price_windows

    post = enable(page, width=band_width)
    response = reply(page, post)
    for row in response["result"]["rows"]:
        row["values"] = [*endpoints, 100.0]
    page.client.resultReceived.emit(post[3], response)
    chart = page.panes[0].chart
    chart._geometry()
    chart._price_range = viewport
    immutable = chart.price_window_snapshot[0]
    painter = Mock()
    draw_price_windows(chart, painter, bands=True)
    paths = [call.args[0] for call in painter.drawPath.call_args_list]
    assert paths
    plot = chart._plot()
    expected_top = chart._y(min(max(endpoints), viewport[1]))
    expected_bottom = chart._y(max(min(endpoints), viewport[0]))
    for path in paths:
        bounds = path.boundingRect()
        assert bounds.width() > 0 and bounds.height() > 0
        assert plot.contains(bounds)
        assert bounds.top() == pytest.approx(expected_top)
        assert bounds.bottom() == pytest.approx(expected_bottom)
        assert all(
            math.isfinite(coordinate)
            for index in range(path.elementCount())
            for coordinate in (path.elementAt(index).x, path.elementAt(index).y)
        )
    # Clipping can rotate the starting vertex. It must not rewrite or relabel
    # signed BOLLOW/BOLHIGH values in the immutable producer snapshot.
    assert chart.price_window_snapshot[0] is immutable
    assert all(row[3][:2] == endpoints for row in immutable)
    labels = page.panes[0].price_windows_request.readout(60000)[0]
    assert f"BOLLOW={endpoints[0]:g}" in labels
    assert f"BOLHIGH={endpoints[1]:g}" in labels


def test_price_main_clipping_opposite_finite_extremes(app):
    from omega.gui.chart_price_windows import _clip_line, _clip_polygon

    bounds = ((0, 0, True), (0, 10, False), (1, 0, True), (1, 10, False))
    segment = _clip_line((0, -1e308), (10, 1e308), bounds)
    assert segment is not None and all(0 <= value <= 10 for point in segment for value in point)
    polygon = _clip_polygon([(0, -1e308), (10, -1e308), (10, 1e308), (0, 1e308)], bounds)
    assert polygon and all(0 <= value <= 10 for point in polygon for value in point)
