"""Database workspace API-only tests; no server, database or exchange is started."""

import importlib.util
import os

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None,
    reason="optional PySide6 GUI dependency is not installed",
)


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["omega-database-tests"])


@pytest.fixture
def page(qt_app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.database_page import DatabasePage

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)

        def __init__(self):
            super().__init__()
            self.attached = True
            self.calls = []

        def request(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    client = Client()
    view = DatabasePage(client)
    yield view
    view.timer.stop()
    view.close()
    view.deleteLater()
    qt_app.processEvents()


def snapshot():
    entries = []
    for symbol in ("BTCUSDT", "ETHUSDT"):
        entries.append(
            {
                "instrument": "binance:usdm:" + symbol,
                "symbol": symbol,
                "market": "usdm",
                "status": "TRADING",
                "collecting_stream": False,
                "collecting_historical": False,
                "first_open_ms": 1700000040000,
                "last_open_ms": 1700000160000,
                "rows": 2,
                "gap_rows": 1,
                "availability": 2 / 3,
                "dummy_rate": 1 / 3,
                "metadata": {"base_asset": symbol[:-4], "quote_asset": "USDT"},
            }
        )
    return {
        "schema_version": 1,
        "source": "database",
        "revision": 1,
        "updated_at_ms": 1700000200000,
        "database": {"state": "ready", "engine": "PostgreSQL + TimescaleDB", "size_bytes": 2048},
        "collection": {"last_fetched": None, "rows_per_second": None},
        "capabilities": {
            "collection_update": True,
            "catalog_refresh": True,
            "gap_refetch": True,
            "local_network_import": True,
        },
        "instruments": entries,
    }


def select_first(page):
    page.accept_snapshot(snapshot())
    page.table.selectRow(0)
    page.client.calls.clear()


def test_unknown_state_never_looks_like_zero_and_actions_start_disabled(page):
    assert page.db_size.text() == "미수신"
    assert page.fetch_speed.text() == "미수신"
    assert not page.apply_button.isEnabled()
    assert not page.refetch_button.isEnabled()
    assert not page.import_button.isEnabled()
    assert not page.compress_button.isEnabled()
    assert not page.reset_button.isEnabled()
    page.set_attached(False)
    assert not page.refresh_button.isEnabled()
    assert "연결 끊김" in page.status.text()


def test_collector_process_is_not_data_readiness_and_requested_modes_survive_stop(page):
    state = snapshot()
    state["collection"]["worker"] = {"state": "gap", "running": True, "ready": False}
    state["instruments"][0].update(
        collection_requested=True,
        historical_requested=True,
        collecting_stream=False,
        collecting_historical=False,
        collection_state="gap",
        data_ready=False,
    )
    page.accept_snapshot(state)
    page.table.selectRow(0)
    assert "실행 / 미준비" in page.worker_state.text()
    assert "미준비" in page.table.item(0, 6).text()
    assert "gap" in page.selection_info.text()
    assert page.stream.isChecked() and page.historical.isChecked()
    state["collection"]["worker"].update(state="stopped", running=False)
    page.accept_snapshot(state)
    assert "정지 / 미준비" in page.worker_state.text()
    assert page.stream.isChecked() and page.historical.isChecked()


def test_absent_readiness_never_looks_ready_and_available_readiness_is_explicit(page):
    state = snapshot()
    page.accept_snapshot(state)
    assert page.worker_state.text() == "미수신"
    assert page.table.item(0, 6).text() == "미수신"
    state["collection"]["worker"] = {"state": "ready", "running": True, "ready": True}
    state["instruments"][0].update(collection_state="ready", data_ready=True)
    page.accept_snapshot(state)
    assert "실행 / 준비됨" in page.worker_state.text()
    assert "준비됨" in page.table.item(0, 6).text()


def test_restored_desired_lookback_is_not_automatic_collection_and_preserves_new_draft(page):
    state = snapshot()
    state["collection"]["manual_apply_required"] = True
    state["instruments"][0].update(
        collection_requested=True,
        historical_requested=True,
        history_lookback_minutes=1500,
    )
    state["instruments"][1]["history_lookback_minutes"] = 700
    page.accept_snapshot(state)
    page.table.selectRow(0)
    assert page.lookback.value() == 1500
    assert "수집 설정 적용 후 재개" in page.worker_state.text()
    page.lookback.setValue(1800)
    page.accept_snapshot(state)
    assert page.lookback.value() == 1800
    page.table.selectRow(1)
    assert page.lookback.value() == 700
    assert not any(args[0] == "POST" for args, _kwargs in page.client.calls)


def test_legacy_layout_instrument_selection_and_collection_contract(page):
    page.accept_snapshot(snapshot())
    assert page.table.rowCount() == 2
    assert page.db_size.text() == "2.00 KiB"
    assert page.db_free.text() == "미수신"
    page.table.selectAll()
    page.stream.setChecked(True)
    page.historical.setChecked(True)
    page.apply_collection()
    args, kwargs = page.client.calls[-1]
    assert args[:2] == ("POST", "/api/database/actions")
    request = args[2]
    assert request["kind"] == "collection.set"
    assert set(request["parameters"]["instruments"]) == {
        "binance:usdm:BTCUSDT",
        "binance:usdm:ETHUSDT",
    }
    assert request["parameters"] | {} == {
        "instruments": page.selected_instruments(),
        "stream": True,
        "historical": True,
        "lookback_minutes": 500,
    }
    assert kwargs["tag"] == "database-action:" + request["id"]
    assert not page.apply_button.isEnabled()


def test_historical_collection_cannot_remain_on_without_stream(page):
    select_first(page)
    page.stream.setChecked(True)
    page.historical.setChecked(True)
    page.stream.setChecked(False)
    assert not page.historical.isChecked()
    assert not page.historical.isEnabled()
    page.apply_collection()
    request = page.client.calls[-1][0][2]
    assert request["parameters"]["stream"] is False
    assert request["parameters"]["historical"] is False


def test_filter_does_not_silently_select_a_different_instrument(page):
    page.accept_snapshot(snapshot())
    page.table.selectRow(1)
    assert page.selected_instruments() == ["binance:usdm:ETHUSDT"]
    page.search.setText("BTC")
    assert page.table.rowCount() == 1
    assert page.selected_instruments() == []
    assert not page.refetch_button.isEnabled()


def test_selection_reflects_current_collection_modes_and_keeps_unsent_edits(page):
    state = snapshot()
    state["instruments"][0].update(collecting_stream=True, collecting_historical=True)
    page.accept_snapshot(state)
    page.table.selectRow(0)
    assert page.stream.isChecked() and page.historical.isChecked()
    page.table.selectRow(1)
    assert not page.stream.isChecked() and not page.historical.isChecked()
    page.stream.setChecked(True)
    page.accept_snapshot(state)
    assert page.stream.isChecked()
    page.apply_collection()
    params = page.client.calls[-1][0][2]["parameters"]
    assert params["instruments"] == ["binance:usdm:ETHUSDT"]
    assert params["stream"] is True and params["historical"] is False


def test_bulk_mixed_modes_require_explicit_choice_before_apply(page):
    from PySide6.QtCore import Qt

    state = snapshot()
    state["instruments"][0].update(collecting_stream=True, collecting_historical=True)
    page.accept_snapshot(state)
    page.table.selectAll()
    assert page.stream.checkState() == Qt.CheckState.PartiallyChecked
    assert page.historical.checkState() == Qt.CheckState.PartiallyChecked
    assert not page.apply_button.isEnabled()
    assert not page.refetch_button.isEnabled() and not page.import_button.isEnabled()
    assert "한 종목씩" in page.refetch_button.toolTip()
    page.client.calls.clear()
    page.apply_collection()
    assert not page.client.calls
    page.stream.setCheckState(Qt.CheckState.Unchecked)
    assert page.apply_button.isEnabled()
    page.apply_collection()
    params = page.client.calls[-1][0][2]["parameters"]
    assert params["stream"] is False and params["historical"] is False


def test_select_displayed_does_not_include_filtered_instruments(page):
    page.accept_snapshot(snapshot())
    page.search.setText("ETH")
    page.select_all.click()
    assert page.selected_instruments() == ["binance:usdm:ETHUSDT"]


def test_unknown_modes_require_explicit_choice_and_gap_rate_is_not_legacy_dummy(page):
    state = snapshot()
    state["instruments"][0].update(
        collecting_stream=None, collecting_historical=None, gap_rate=None, dummy_rate=0.5
    )
    page.accept_snapshot(state)
    page.table.selectRow(0)
    assert not page.apply_button.isEnabled()
    assert page.channels.item(0, 2).text() == "66.667% / 미수신"
    assert "실관측" in page.channels.horizontalHeaderItem(2).text()
    page.stream.setChecked(False)
    assert page.apply_button.isEnabled()
    assert page.lookback.maximum() == 20000


def test_old_complete_slice_discloses_internal_ratio_and_exact_bounded_range(page):
    select_first(page)
    state = snapshot()
    state["selected"] = {
        "instrument": "binance:usdm:BTCUSDT",
        "channels": {
            "ohlcv": {
                "supported": True,
                "first_open_ms": 1700000040000,
                "last_open_ms": 1700000580000,
                "rows": 10,
                "availability": 1,
                "gap_rate": 0,
                "scope": "internal gaps; exact ranges bounded to latest 20000 minutes",
                "range_window": {"start_ms": 1700000040000, "end_ms": 1700000640000},
                "available_ranges": [[1700000040000, 1700000640000]],
                "gap_ranges": [],
            }
        },
    }
    page.accept_snapshot(state)
    assert page.channels.item(0, 2).text() == "100.000% / 0.000%"
    text = page.quality_scope.text()
    assert "비율 대상: 2023/11/14 22:14 ~ 2023/11/14 22:23 UTC" in text
    assert "최신 관측: 2023/11/14 22:23 UTC" in text
    assert "구간 목록 대상: 2023/11/14 22:14 ~ 2023/11/14 22:24 UTC" in text
    assert "20,000분" in text and "시작 포함, 끝 제외" in text
    assert "내부 결측" in text and "현재까지의 완전성" in text
    page.set_presentation("light", "en")
    assert "Latest observation: 2023/11/14 22:23" in page.quality_scope.text()
    assert "internal gaps" in page.quality_scope.text()


def test_missing_range_window_stays_unknown_and_truncated_ranges_show_count(page):
    select_first(page)
    assert "구간 목록 대상: 미수신 ~ 미수신 UTC" in page.quality_scope.text()
    ranges = [[1700000040000 + i * 120000, 1700000100000 + i * 120000] for i in range(7)]
    text = page._ranges(ranges)
    assert "그 외 3구간" in text
    assert page._time(ranges[4][0]) not in text
    page.set_presentation("light", "en")
    assert "3 more ranges" in page._ranges(ranges)
    assert page._ranges(None) == "Not received"


def test_selected_channels_show_missing_not_synthetic_interpolation(page):
    select_first(page)
    response = snapshot()
    response["selected"] = {
        "instrument": "binance:usdm:BTCUSDT",
        "channels": {
            "ohlcv": {
                "supported": True,
                "availability": 2 / 3,
                "dummy_rate": 1 / 3,
                "first_open_ms": 1700000040000,
                "last_open_ms": 1700000160000,
                "available_ranges": [[1700000040000, 1700000100000]],
                "gap_ranges": [[1700000100000, 1700000160000]],
            },
            "depth": {"supported": False},
        },
    }
    page.accept_snapshot(response)
    assert "66.667%" in page.channels.item(0, 2).text()
    assert "33.333%" in page.channels.item(0, 2).text()
    assert page.channels.item(1, 1).text() == "미지원"
    page.refetch()
    request = page.client.calls[-1][0][2]
    assert request["kind"] == "gaps.refetch"
    assert request["parameters"] == {"instrument": "binance:usdm:BTCUSDT", "lookback_minutes": 500}


def test_peer_import_password_is_only_in_transient_request_and_cleared(page):
    select_first(page)
    page.host.setText("192.168.1.12")
    page.password.setText("synthetic-password")
    page.import_peer()
    args, kwargs = page.client.calls[-1]
    request = args[2]
    assert request["kind"] == "peer.import"
    assert request["parameters"]["password"] == "synthetic-password"
    assert request["parameters"]["host"] == "192.168.1.12"
    assert not page.password.text()
    page.client.resultReceived.emit(
        kwargs["tag"], {"id": request["id"], "state": "failed", "error": "synthetic-password"}
    )
    assert "synthetic-password" not in page.status.text()
    assert not hasattr(page, "_parameters")


@pytest.mark.parametrize("host", ["8.8.8.8", "example.com", "0.0.0.0", "224.0.0.1"])
def test_peer_import_rejects_nonlocal_or_nonliteral_host(page, host):
    select_first(page)
    page.host.setText(host)
    page.password.setText("synthetic-password")
    page.import_peer()
    assert not page.client.calls
    assert not page.password.text()


def test_action_timeout_queries_known_id_without_reposting(page):
    select_first(page)
    page.refetch()
    request = page.client.calls[-1][0][2]
    tag = "database-action:" + request["id"]
    page.client.failed.emit(tag, "timeout with arbitrary server details")
    assert page._busy
    assert "결과 미확인" in page.status.text()
    assert "arbitrary" not in page.status.text()
    page.refresh()
    calls = page.client.calls
    assert len([args for args, _ in calls if args[0] == "POST"]) == 1
    assert any(args[:2] == ("GET", "/api/database/actions/" + request["id"]) for args, _ in calls)


def test_stale_snapshot_reply_cannot_replace_new_market(page):
    original_tag = page._expected_tag
    page.market.setCurrentIndex(1)
    page.client.resultReceived.emit(original_tag, snapshot())
    assert page.table.rowCount() == 0
    assert not page._have_snapshot


def test_capability_false_and_disconnection_block_mutations(page):
    state = snapshot()
    state["capabilities"] = {"gap_refetch": False, "collection_update": "true"}
    page.accept_snapshot(state)
    page.table.selectRow(0)
    page.client.calls.clear()
    page.refetch()
    page.apply_collection()
    assert not page.client.calls
    page.accept_snapshot(snapshot())
    page.set_attached(False)
    page.refetch()
    assert not [args for args, _ in page.client.calls if args[0] == "POST"]


def test_resize_and_language_keep_layout_interactive_without_database(page, qt_app):
    page.accept_snapshot(snapshot())
    page.show()
    for width, height in ((1360, 860), (900, 600), (1360, 860)):
        page.resize(width, height)
        qt_app.processEvents()
        assert not page.grab().isNull()
        assert page._compact is (width < 1050)
        assert page.splitter.count() == (0 if width < 1050 else 2)
    page.set_presentation("dark", "en")
    assert page.table.horizontalHeaderItem(0).text() == "Instrument"
    assert page.db_free.text() == "Not received"


def populated_snapshot():
    state = snapshot()
    state["source"] = "demo"
    template = state["instruments"][0]
    state["instruments"] = [
        template
        | {
            "instrument": f"binance:usdm:LONG_INSTRUMENT_{i:02d}_USDT",
            "symbol": f"LONG_INSTRUMENT_{i:02d}_USDT",
        }
        for i in range(24)
    ]
    state["selected"] = {
        "instrument": state["instruments"][0]["instrument"],
        "metadata": template["metadata"],
        "channels": {
            "ohlcv": template
            | {
                "supported": True,
                "gap_rate": 1 / 3,
                "range_window": {"start_ms": 1700000040000, "end_ms": 1700000220000},
                "available_ranges": [[1700000040000, 1700000100000]],
                "gap_ranges": [[1700000100000, 1700000160000]],
            },
            "depth": {"supported": False},
            "agg_trade": {"supported": False},
            "metrics": {"supported": False},
        },
    }
    return state


def test_collection_units_and_partial_repair_result_are_explicit(page):
    state = snapshot()
    state["collection"] = {
        "last_fetched": 1700000040000,
        "rows_per_second": 12.345678,
        "remaining_ranges": 2,
        "eta_ms": 3_661_000,
    }
    page.accept_snapshot(state)
    assert page.last_fetched.text() == "2023/11/14 22:14 UTC"
    assert page.fetch_speed.text() == "12.346 / s"
    assert page.eta.text() == "01:01:01"
    page._action_id = "repair-1"
    page._busy = True
    page._result(
        "database-action:repair-1",
        {"state": "complete", "result": {"inserted_rows": 3, "remaining_minutes": 2}},
    )
    assert "추가 3봉" in page.status.text()
    assert "미복구 2분" in page.status.text()
    assert not page._busy


def test_rejected_action_releases_controls_without_reposting(page):
    page.accept_snapshot(snapshot())
    page.table.selectRow(0)
    page._action_id = "invalid-peer"
    page._busy = True
    page._result(
        "database-action:invalid-peer",
        {"state": "rejected", "error": "INVALID_DATABASE_ACTION"},
    )
    assert not page._busy
    assert page.import_button.isEnabled()
    assert not [args for args, _ in page.client.calls if args[0] == "POST"]


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_compact_shell_has_selectable_rows_and_readable_detail_tab(page, qt_app, theme):
    from PySide6.QtWidgets import QLabel, QVBoxLayout, QWidget

    from omega.gui.presentation import configure_fonts, stylesheet

    configure_fonts(qt_app)
    shell = QWidget()
    shell.setStyleSheet(stylesheet(theme))
    layout = QVBoxLayout(shell)
    layout.setContentsMargins(0, 0, 0, 0)
    toolbar = QLabel("Database shell toolbar")
    toolbar.setFixedHeight(85)
    layout.addWidget(toolbar)
    layout.addWidget(page, 1)
    footer = QLabel("DEMO")
    footer.setFixedHeight(25)
    layout.addWidget(footer)
    page.set_shell_embedded(True)
    page.set_presentation(theme, "ko")
    page.accept_snapshot(populated_snapshot())
    shell.resize(800, 560)
    shell.show()
    qt_app.processEvents()
    assert shell.size().width() == 800
    assert shell.size().height() == 560
    assert not page.title.isVisible()
    assert page.compact_tabs.count() == 3
    assert page.table.viewport().height() >= page.table.rowHeight(0) * 3
    assert page.table.isVisible() and not page.detail_panel.isVisible()
    page.table.selectRow(0)
    selected = page.selected_instruments()
    page.compact_tabs.setCurrentIndex(1)
    qt_app.processEvents()
    assert page.detail_panel.isVisible() and not page.table.isVisible()
    assert page.detail_panel.viewport().height() >= 250
    assert page.channels.rowCount() == 4
    assert "66.667%" in page.channels.item(0, 2).text()
    assert "내부 결측" in page.quality_scope.text()
    page.compact_tabs.setCurrentIndex(2)
    assert page.left_panel.isVisible()
    shell.resize(1360, 860)
    qt_app.processEvents()
    assert page.splitter.count() == 2 and page.content_splitter.count() == 2
    assert page.selected_instruments() == selected
    assert page.table.isVisible() and page.detail_panel.isVisible()
    page.set_shell_embedded(False)
    assert page.title.isVisible()
    page.setParent(None)
    shell.close()
    shell.deleteLater()
