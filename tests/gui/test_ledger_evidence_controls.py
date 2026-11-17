"""Qt history pages use real typed facts and the shared ZIP exporter, never financial math."""

import hashlib
import importlib.util
import json
import os
import time
import zipfile
from copy import deepcopy
from decimal import Decimal as D
from urllib.parse import parse_qs, urlsplit

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt is not installed"
)


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["ledger-evidence-controls"])


@pytest.fixture
def view(app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.ledger_history import LedgerHistoryWidget

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        connectionChanged = Signal(bool)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls = []
            self.handler = None

        def request(self, method, path, payload=None, *, tag):
            assert method == "GET" and payload is None
            self.calls.append((path, tag))
            if self.handler:
                self.resultReceived.emit(tag, self.handler(path))

    widget = LedgerHistoryWidget(Client())
    widget.update_state({"core": {"epoch": "1" * 32, "capabilities": {"margin_trading": True}}})
    widget.set_accounts(["account"])
    widget.account.setCurrentIndex(1)
    yield widget
    task = widget.job
    widget.close()
    if task:
        task.thread.join(2)
    app.processEvents()
    widget.deleteLater()


@pytest.fixture
def source():
    from omega.domain.margin import (
        FillEvent,
        FundingCashflow,
        MarginAccount,
        MarginMode,
        MarginPosition,
        apply_fill,
        apply_funding,
    )
    from omega.domain.models import Instrument, Market
    from omega.runtime.margin_ledger import LedgerPages, LedgerQuery

    instrument = Instrument("binance", Market.USDM, "BTCUSDT")
    account = MarginAccount(
        "account",
        "USDT",
        D(1000),
        (MarginPosition(instrument, "strategy", MarginMode.CROSS, 10, D(100)),),
    )
    account = apply_funding(
        account,
        FundingCashflow(
            funding_id="zero",
            instrument=instrument,
            owner_id="strategy",
            amount=D(0),
            rate=D("-0.0100"),
            reference_price=D(100),
            signed_quantity=D(0),
            event_time_ms=0,
            source_available_at_ms=0,
            available_at_ms=0,
            source_kind="scenario_rate",
            source_sha256="a" * 64,
            policy_sha256="b" * 64,
        ),
    ).account
    for index in range(130):
        account = apply_fill(
            account,
            FillEvent(
                f"fill-{index}",
                f"order-{index}",
                instrument,
                "strategy",
                D(1 if index % 2 == 0 else -1),
                D(100),
                D(".1"),
                "USDT",
                (index + 1) * 60000,
                (index + 2) * 60000,
            ),
        ).account
    pages = LedgerPages("c" * 64)
    state = {"account": account, "epoch": "1" * 32}

    def read(path):
        query = {key: values[0] for key, values in parse_qs(urlsplit(path).query).items()}
        for key in ("offset", "limit", "start_ms", "end_ms"):
            if key in query:
                query[key] = int(query[key])
        return pages.page("d" * 64, state["account"], LedgerQuery(**query)) | {
            "core_epoch": state["epoch"]
        }

    state["read"] = read
    return state


def response(view, source):
    path, tag = view.client.calls[-1]
    view.client.resultReceived.emit(tag, source["read"](path))


def spin(app, predicate, *, timeout=4):
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        app.processEvents()
        time.sleep(0.002)
    assert predicate()


def test_real_131_facts_page_128_next_previous_and_explicit_snapshot(view, source):
    from omega.domain.margin import FillEvent, apply_fill

    view.refresh()
    response(view, source)
    token = view.page["snapshot"]
    assert view.table.rowCount() == 128 and view.table.item(0, 1).text() == "funding"
    assert view.table.item(0, 6).text() == "0"
    assert "ledger 131 / capacity" in view.counts.text()
    assert view.next_button.isEnabled() and not view.previous_button.isEnabled()
    original = source["account"]
    source["account"] = apply_fill(
        original,
        FillEvent(
            "appended",
            "new-order",
            original.positions[0].instrument,
            "strategy",
            D(1),
            D(100),
            D(".1"),
            "USDT",
            9000000,
            9060000,
        ),
    ).account
    view.next_button.click()
    assert parse_qs(urlsplit(view.client.calls[-1][0]).query)["snapshot"] == [token]
    response(view, source)
    assert view.table.rowCount() == 3 and view.table.item(0, 0).text() == "129"
    assert not view.next_button.isEnabled()
    view.previous_button.click()
    response(view, source)
    assert view.page["offset"] == 0 and view.table.rowCount() == 128
    view.refresh_button.click()
    assert "snapshot=" not in view.client.calls[-1][0]
    response(view, source)
    assert view.page["ledger_total"] == 132 and view.page["snapshot"] != token


@pytest.mark.parametrize("invalidate", ["filter", "selection", "epoch", "disconnect"])
def test_late_page_is_fenced_and_old_rows_explicitly_unconfirmed(view, source, invalidate):
    view.refresh()
    response(view, source)
    view.next_button.click()
    path, tag = view.client.calls[-1]
    if invalidate == "filter":
        view.period.setChecked(True)
    elif invalidate == "selection":
        view.set_accounts(["other"])
    elif invalidate == "epoch":
        view.update_state({"core": {"epoch": "2" * 32, "capabilities": {"margin_trading": True}}})
    else:
        view.client.attached = False
        view.client.connectionChanged.emit(False)
    view.client.resultReceived.emit(tag, source["read"](path))
    assert view.table.rowCount() == 128 and not view.page_current
    assert not view.next_button.isEnabled()
    assert "미확인" in view.status.text()


def test_utc_filter_is_inclusive_start_exclusive_end_and_change_requires_new_snapshot(view, source):
    from PySide6.QtCore import QDateTime, QTimeZone

    view.period.setChecked(True)
    for field, value in ((view.start, 60000), (view.end, 180000)):
        field.setDateTime(QDateTime.fromMSecsSinceEpoch(value, QTimeZone(b"UTC")))
    view.refresh()
    response(view, source)
    assert view.page["total"] == 2
    assert [row["fact"]["event_time_ms"] for row in view.page["rows"]] == [60000, 120000]
    assert "UTC" in view.start.text() and "UTC" in view.end.text()
    calls = len(view.client.calls)
    view.end.setDateTime(view.start.dateTime())
    view.refresh()
    assert len(view.client.calls) == calls
    assert not view.page_current


@pytest.mark.parametrize("bad", ["unavailable", "epoch", "bool", "failure"])
def test_unavailable_malformed_and_failure_do_not_become_empty_history(view, source, bad):
    view.refresh()
    response(view, source)
    view.refresh()
    path, tag = view.client.calls[-1]
    data = source["read"](path)
    if bad == "failure":
        view.client.failed.emit(tag, "timeout")
    else:
        if bad == "unavailable":
            from omega.runtime.margin_ledger import LedgerQuery, unavailable_ledger

            data = unavailable_ledger("account", LedgerQuery(), "unconfirmed") | {
                "core_epoch": "1" * 32
            }
        elif bad == "epoch":
            data["core_epoch"] = "old"
        else:
            data["ledger_total"] = True
        view.client.resultReceived.emit(tag, data)
    assert view.table.rowCount() == 128 and not view.page_current
    assert not view.next_button.isEnabled()


def test_shared_export_actual_worker_all_rows_signed_values_and_gui_thread_get(
    app, view, source, tmp_path
):
    from PySide6.QtCore import QThread

    owner_thread = app.thread()
    observed = []

    def read(path):
        observed.append(QThread.currentThread() == owner_thread)
        return source["read"](path)

    view.client.handler = read
    destination = tmp_path / "ledger.zip"
    view.start_export(destination)
    task = view.job
    spin(app, lambda: view.job is None)
    task.thread.join(2)
    assert not task.thread.is_alive() and observed == [True, True]
    with zipfile.ZipFile(destination) as archive:
        assert set(archive.namelist()) == {"ledger.csv", "manifest.json"}
        csv = archive.read("ledger.csv").decode()
        manifest = json.loads(archive.read("manifest.json"))
    assert "fill-129" in csv and "-1" in csv and "-0.0100" in csv
    assert manifest["rows"] == 131
    assert manifest["csv_sha256"] == hashlib.sha256(csv.encode()).hexdigest()
    assert not task.staging.exists()


def test_export_invalid_second_page_preserves_prior_zip(app, view, source, tmp_path):
    def read(path):
        page = source["read"](path)
        if page["offset"]:
            page["core_epoch"] = "2" * 32
        return page

    view.client.handler = read
    destination = tmp_path / "ledger.zip"
    destination.write_bytes(b"previous")
    view.start_export(destination)
    task = view.job
    spin(app, lambda: view.job is None)
    task.thread.join(2)
    assert destination.read_bytes() == b"previous"
    assert not task.staging.exists()
    assert "실패" in view.status.text()


def test_selection_change_after_last_page_before_completion_never_publishes(view, tmp_path):
    from omega.gui.ledger_export import LedgerExportTask

    destination = tmp_path / "ledger.zip"
    destination.write_bytes(b"previous")
    task = LedgerExportTask(destination, account_id="account", filters={}, context=view._context())
    task.staging.write_bytes(b"completed-shared-export")
    view.job = task
    view.period.setChecked(True)
    view._export_done(task, {}, "")
    assert destination.read_bytes() == b"previous" and not task.staging.exists()


def test_history_entry_route_preserves_recent_tab_and_explicit_account_selection(view, source):
    from omega.gui.history_page import HistoryPage
    from omega.runtime.margin_store import account_document

    page = HistoryPage(view.client)
    try:
        page.update_state(
            {
                "core": {
                    "epoch": "1" * 32,
                    "capabilities": {"margin_trading": True},
                }
            }
        )
        view.client.resultReceived.emit(
            page.margin_history.pending[0],
            {
                "scope": "virtual_margin_v2_ohlcv_proxy",
                "core_epoch": "1" * 32,
                "accounts": [
                    {
                        "account_id": "account",
                        "state": "ready",
                        "account": account_document(source["account"], ledger_limit=128),
                    }
                ],
            },
        )
        assert page.margin_history.table.rowCount() == 128
        assert page.ledger_history.account.currentData() is None
        page.margin_history.full_ledger_button.click()
        assert page.history_tabs.currentWidget() is page.ledger_history
        assert page.ledger_history.account.currentData() == "account"
        assert page.ledger_history.pending
    finally:
        page.ledger_history.close()
        page.close()


@pytest.mark.parametrize("width", [800, 1360])
def test_page_controls_resize_without_expanding_window(app, view, source, width):
    view.refresh()
    response(view, source)
    view.resize(width, 800)
    view.show()
    app.processEvents()
    assert view.width() == width
    for control in (view.account, view.start, view.end, view.export_button):
        assert control.geometry().right() < view.width()
    assert view.table.rowCount() <= 128


@pytest.mark.parametrize("change", ["filter", "epoch", "disconnect", "close"])
def test_export_cancel_keeps_previous_zip_and_late_reply_cannot_publish(
    app, view, source, tmp_path, change
):
    destination = tmp_path / "ledger.zip"
    destination.write_bytes(b"previous-export")
    view.start_export(destination)
    task = view.job
    spin(app, lambda: bool(view.export_requests))
    path, tag = view.client.calls[-1]
    if change == "filter":
        view.period.setChecked(True)
    elif change == "epoch":
        view.update_state({"core": {"epoch": "2" * 32, "capabilities": {"margin_trading": True}}})
    elif change == "disconnect":
        view.client.attached = False
        view.client.connectionChanged.emit(False)
    else:
        view.close()
    view.client.resultReceived.emit(tag, source["read"](path))
    spin(app, lambda: view.job is None)
    task.thread.join(2)
    assert not task.thread.is_alive()
    assert destination.read_bytes() == b"previous-export"
    assert not task.staging.exists()


def budget():
    return {
        "schema_version": 1,
        "scope": "host_causal_evidence_accounting",
        "state": "warning",
        "ready": True,
        "accepts_new_reservation": False,
        "accepts_new_pin": True,
        "limits": {
            "max_bytes": 536870912,
            "max_records": 100000,
            "max_pins": 2048,
            "warning_percent": 80,
        },
        "charged_bytes": 1234,
        "charged_records": 3,
        "active_pins": 2,
        "uncertain_reservations": 1,
        "uncertain_reserved_bytes": 12,
        "remaining_bytes": 22,
        "remaining_records": 100,
        "remaining_pins": 2046,
        "reservation_headroom_bytes": 2097152,
        "consumed_receipt_allowance_bytes": 4194304,
        "consumed_receipt_allowance_records": 2048,
    }


def test_evidence_exact_cached_scope_unknown_and_stale_never_display_disk_free():
    from omega.gui.evidence_status import COUNTS, evidence_status

    data = budget()
    original = deepcopy(data)
    shown = evidence_status(data, current=True, language="en")
    assert "1234 / 536870912" in shown and "new reservation no" in shown.casefold()
    assert "next core start" in shown and "not total disk/model/training" in shown
    data.update(
        state="unavailable",
        ready=False,
        accepts_new_reservation=False,
        accepts_new_pin=False,
        **dict.fromkeys(COUNTS),
    )
    assert "Bytes unknown /" in evidence_status(data, language="en")
    assert "previous observation" in evidence_status(data, language="en")
    assert original["charged_bytes"] == 1234


@pytest.mark.parametrize("state", ["ready", "warning", "full"])
def test_available_evidence_requires_known_counts_and_ready(state):
    from omega.gui.evidence_status import evidence_status

    data = budget() | {"state": state}
    assert not evidence_status(data).startswith("증거 저장 상태 미확인")
    for changes in ({"ready": False}, {"charged_bytes": None}, {"remaining_pins": None}):
        assert evidence_status(data | changes).startswith("증거 저장 상태 미확인")


def test_unavailable_evidence_cannot_advertise_acceptance_or_known_counters():
    from omega.gui.evidence_status import COUNTS, evidence_status

    data = (
        budget()
        | dict.fromkeys(COUNTS)
        | {
            "state": "unavailable",
            "ready": False,
            "accepts_new_reservation": False,
            "accepts_new_pin": False,
        }
    )
    for changes in (
        {"ready": True},
        {"accepts_new_pin": True},
        {"accepts_new_reservation": True},
        {"charged_records": 0},
    ):
        assert evidence_status(data | changes).startswith("증거 저장 상태 미확인")


@pytest.mark.parametrize("destroy", [False, True])
def test_worker_cleanup_after_last_read_does_not_need_queued_completion(
    app, source, tmp_path, monkeypatch, destroy
):
    import threading

    import shiboken6
    from PySide6.QtWidgets import QWidget

    from omega.gui.ledger_export import LedgerExportTask
    from omega.runtime import ledger_client

    parent = QWidget()
    destination = tmp_path / "ledger.zip"
    destination.write_bytes(b"previous")
    task = LedgerExportTask(
        destination, account_id="account", filters={}, context=(), parent=parent
    )
    task.state.read_page = lambda identity, query: source["read"](
        ledger_client.ledger_url(identity, query)
    )
    completed, release = threading.Event(), threading.Event()
    export = ledger_client.export_margin_ledger

    def paused_after_final_page(*args, **kwargs):
        result = export(*args, **kwargs)
        completed.set()
        assert release.wait(3)
        return result

    monkeypatch.setattr(ledger_client, "export_margin_ledger", paused_after_final_page)
    task.start()
    assert completed.wait(3) and task.staging.exists()
    if destroy:
        shiboken6.delete(parent)
    else:
        task.cancel()
    release.set()
    task.thread.join(3)
    # Intentionally no processEvents: a closed application's queued slot cannot clean up.
    assert not task.thread.is_alive() and not task.staging.exists()
    assert task.cleanup_error is None and destination.read_bytes() == b"previous"
    if not destroy:
        shiboken6.delete(parent)


def test_worker_has_no_qt_target_or_state_and_only_gui_emits_after_thread_exit(
    app, view, source, tmp_path, monkeypatch
):
    import threading

    from PySide6.QtCore import QObject

    from omega.gui import ledger_export

    gui_thread = threading.get_ident()
    observations = []
    run = ledger_export._run_export

    def trap(state, account_id, filters):
        assert threading.get_ident() != gui_thread
        assert not isinstance(state, QObject)
        assert set(vars(state)) == {
            "staging",
            "condition",
            "cancelled",
            "ticket",
            "finished",
            "result",
            "error",
            "cleanup_error",
        }
        assert all(not isinstance(value, QObject) for value in vars(state).values())
        assert type(account_id) is str and type(filters) is dict
        assert state.read_page.__self__ is state
        run(state, account_id, filters)

    monkeypatch.setattr(ledger_export, "_run_export", trap)
    view.client.handler = source["read"]
    view.start_export(tmp_path / "boundary.zip")
    task = view.job
    task.readRequested.connect(lambda ticket: observations.append(("read", threading.get_ident())))
    task.completed.connect(
        lambda *_args: observations.append(
            (
                "complete",
                threading.get_ident(),
                task.thread.is_alive(),
                task.timer.isActive(),
                task._parent_connection,
            )
        )
    )
    spin(app, lambda: view.job is None)
    assert observations[:2] == [("read", gui_thread), ("read", gui_thread)]
    assert observations[-1] == ("complete", gui_thread, False, False, None)


def test_finished_state_does_not_publish_or_destroy_bridge_until_worker_has_exited(
    app, view, source, tmp_path, monkeypatch
):
    import threading

    import shiboken6

    from omega.gui import ledger_export

    finished, release = threading.Event(), threading.Event()
    run = ledger_export._run_export

    def delayed_exit(state, account_id, filters):
        run(state, account_id, filters)
        finished.set()
        assert release.wait(3)

    monkeypatch.setattr(ledger_export, "_run_export", delayed_exit)
    view.client.handler = source["read"]
    destination = tmp_path / "finished.zip"
    destination.write_bytes(b"previous")
    view.start_export(destination)
    task = view.job
    try:
        spin(app, finished.is_set)
        for _ in range(3):
            time.sleep(0.025)
            app.processEvents()
        assert task.state.finished and task.thread.is_alive()
        assert shiboken6.isValid(task) and view.job is task
        assert task.staging.exists() and destination.read_bytes() == b"previous"
    finally:
        release.set()
    spin(app, lambda: view.job is None)
    assert not task.thread.is_alive() and destination.read_bytes() != b"previous"


def test_completed_bridge_then_parent_destruction_gc_has_no_worker_qt_reference(
    app, source, tmp_path
):
    import gc
    import weakref

    import shiboken6
    from PySide6.QtCore import QCoreApplication, QEvent, Qt
    from PySide6.QtWidgets import QWidget

    from omega.gui.ledger_export import LedgerExportTask
    from omega.runtime.ledger_client import ledger_url

    for index in range(4):
        parent = QWidget()
        task = LedgerExportTask(
            tmp_path / f"lifecycle-{index}.zip",
            account_id="account",
            filters={},
            context=(),
            parent=parent,
        )
        task.state.read_page = lambda identity, query: source["read"](ledger_url(identity, query))
        reference = weakref.ref(task.state)
        assert not task.timer.isActive()
        done = []
        task.completed.connect(lambda *_args, observed=done: observed.append(True))
        task.completed.connect(task.deleteLater, Qt.ConnectionType.QueuedConnection)
        task.start()
        spin(app, lambda observed=done: bool(observed))
        assert not task.thread.is_alive() and task._parent_connection is None
        app.processEvents()  # Deliver the explicitly queued deleteLater call first.
        QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
        assert not shiboken6.isValid(task)
        del task
        gc.collect()
        assert reference() is None  # Living parent has no retained state callback.
        shiboken6.delete(parent)
        gc.collect()


def test_raw_parent_delete_after_poll_before_queued_completion_cleans_staging(
    view, tmp_path, monkeypatch
):
    import shiboken6
    from PySide6.QtWidgets import QWidget

    from omega.gui.ledger_history import LedgerHistoryWidget
    from omega.runtime import ledger_client

    def fixture_export(read_page, destination, **kwargs):
        destination.write_bytes(b"completed stage")
        return {}

    monkeypatch.setattr(ledger_client, "export_margin_ledger", fixture_export)
    parent = QWidget()
    panel = LedgerHistoryWidget(view.client, parent)
    panel.update_state(
        {
            "core": {
                "epoch": "1" * 32,
                "capabilities": {"margin_trading": True},
            }
        }
    )
    panel.set_accounts(["account"])
    panel.account.setCurrentIndex(1)
    destination = tmp_path / "raw-delete.zip"
    destination.write_bytes(b"previous export")
    panel.start_export(destination)
    task = panel.job
    task.thread.join(3)
    assert not task.thread.is_alive() and task.state.finished
    task.timer.stop()
    task._poll()  # Queues the production completion slot; intentionally do not deliver it.
    assert task._parent_connection is None and panel.job is task
    assert task.staging.exists() and destination.read_bytes() == b"previous export"
    shiboken6.delete(parent)
    # Neither a queued completion nor another GUI event is available to clean up.
    assert not shiboken6.isValid(task)
    assert not task.staging.exists() and task.state.cancelled
    assert task.cleanup_error is None and destination.read_bytes() == b"previous export"


@pytest.mark.parametrize(
    "key,value",
    [
        ("schema_version", True),
        ("scope", "disk"),
        ("charged_bytes", False),
        ("active_pins", -1),
        ("ready", 1),
        ("state", []),
        ("state", {}),
        ("consumed_receipt_allowance_records", 0),
    ],
)
def test_malformed_evidence_is_unknown_not_zero(key, value):
    from omega.gui.evidence_status import evidence_status

    data = budget() | {key: value}
    assert evidence_status(data, language="en").startswith("Evidence storage unknown")


def test_causal_cached_evidence_display_get_only_epoch_disconnect_and_unknown(view):
    from omega.gui.causal_controls import CausalControls

    panel = CausalControls(view.client)
    try:
        panel.update_state(
            {
                "core": {
                    "epoch": "1" * 32,
                    "capabilities": {"causal_inference": True},
                }
            }
        )
        panel.refresh()
        tag = view.client.calls[-1][1]
        view.client.resultReceived.emit(
            tag,
            {
                "schema_version": 1,
                "scope": "committed_inference_service",
                "live_orders_enabled": False,
                "strategy_delivery_enabled": False,
                "core_epoch": "1" * 32,
                "revision": 0,
                "server_time_ms": 120000,
                "state": "disabled",
                "ready": True,
                "storage_available": True,
                "actor_alive": True,
                "active_deployment_id": None,
                "deployments": [],
                "streams": [],
                "operations": [],
                "error": None,
                "evidence_storage": budget(),
            },
        )
        assert "1234 / 536870912" in panel.evidence_status.text()
        assert "현재 조회" in panel.evidence_status.text()
        panel.set_attached(False)
        assert "이전 조회" in panel.evidence_status.text()
        panel.update_state(
            {
                "core": {
                    "epoch": "2" * 32,
                    "capabilities": {"causal_inference": True},
                }
            }
        )
        assert "미확인" in panel.evidence_status.text()
    finally:
        panel.timer.stop()
        panel.close()
        panel.deleteLater()
