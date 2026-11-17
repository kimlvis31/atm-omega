"""Validated replies have bounded ownership across many unrelated GUI consumers."""

import importlib.util
import json
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

    return QApplication.instance() or QApplication(["result fanout"])


class Reply:
    def __init__(self, raw):
        self.raw, self.deleted, self.reads = raw, False, 0

    def attribute(self, _attribute):
        return 200

    def error(self):
        from PySide6.QtNetwork import QNetworkReply

        return QNetworkReply.NetworkError.NoError

    def readAll(self):
        self.reads += 1
        return self.raw

    def deleteLater(self):
        self.deleted = True


def test_validated_catalog_is_not_copied_for_each_unrelated_page(qt_app, tmp_path):
    from omega.gui.connection import CoreClient

    client = CoreClient(tmp_path)
    receivers = [[] for _ in range(27)]
    for seen in receivers:
        client.resultReceived.connect(lambda tag, data, seen=seen: seen.append((tag, data)))
    payload = {
        "instruments": [
            {"symbol": f"TEST{index}USDT", "metadata": {"limits": [None, True, "0.0100"]}}
            for index in range(128)
        ],
        "source": "database",
    }
    reply = Reply(json.dumps(payload).encode())
    tag = "database-snapshot:spot:"
    client._pending[tag] = reply
    try:
        client._finished(reply, tag, client._generation)
        assert all(len(seen) == 1 and seen[0][0] == tag for seen in receivers)
        delivered = receivers[0][0][1]
        assert delivered == payload and isinstance(delivered, dict)
        # One parsed response is retained once, even if dozens of pages discard
        # its tag. Mutating consumers must explicitly copy their own draft.
        assert all(seen[0][1] is delivered for seen in receivers)
        assert reply.reads == 1 and reply.deleted and tag not in client._pending
    finally:
        client.stop()


@pytest.mark.parametrize("raw", [b"[]", b"null", b"not-json", b"[1,2,3]"])
def test_nonobject_replies_are_rejected_before_shared_delivery(qt_app, tmp_path, raw):
    from omega.gui.connection import CoreClient

    client = CoreClient(tmp_path)
    accepted, failed = [], []
    client.resultReceived.connect(lambda tag, data: accepted.append((tag, data)))
    client.failed.connect(lambda tag, error: failed.append((tag, error)))
    reply = Reply(raw)
    try:
        client._finished(reply, "database-snapshot:spot:", client._generation)
        assert accepted == [] and len(failed) == 1 and reply.deleted
    finally:
        client.stop()


def test_retired_connection_response_never_enters_the_new_consumers(qt_app, tmp_path):
    from omega.gui.connection import CoreClient

    client = CoreClient(tmp_path)
    accepted = []
    client.resultReceived.connect(lambda tag, data: accepted.append((tag, data)))
    reply = Reply(b'{"source":"old-generation"}')
    try:
        client._finished(reply, "database-snapshot:spot:", client._generation - 1)
        assert accepted == [] and reply.reads == 0 and reply.deleted
    finally:
        client.stop()


def test_oversized_reply_is_rejected_before_shared_delivery(qt_app, tmp_path):
    from omega.gui.connection import MAX_RESPONSE_BYTES, CoreClient

    client = CoreClient(tmp_path)
    accepted, failed = [], []
    client.resultReceived.connect(lambda tag, data: accepted.append((tag, data)))
    client.failed.connect(lambda tag, error: failed.append((tag, error)))
    reply = Reply(b" " * (MAX_RESPONSE_BYTES + 1))
    try:
        client._finished(reply, "database-snapshot:spot:", client._generation)
        assert accepted == [] and len(failed) == 1 and reply.deleted
    finally:
        client.stop()


def test_database_draft_edits_do_not_mutate_the_shared_response(qt_app, tmp_path):
    from PySide6.QtCore import QCoreApplication, QEvent, Qt

    from omega.gui.connection import CoreClient
    from omega.gui.database_page import DatabasePage

    client = CoreClient(tmp_path)
    page = DatabasePage(client)
    page.timer.stop()
    page.market.setCurrentIndex(page.market.findData("spot"))
    page.set_attached(True)
    payload = {
        "schema_version": 1,
        "source": "database",
        "database": {"state": "ready"},
        "collection": {},
        "capabilities": {"collection_update": True},
        "instruments": [
            {
                "instrument": "binance:spot:BTCUSDT",
                "symbol": "BTCUSDT",
                "market": "spot",
                "status": "TRADING",
                "collecting_stream": False,
                "collecting_historical": False,
                "collection_requested": False,
                "historical_requested": False,
                "history_lookback_minutes": 50,
                "metadata": {"base_asset": "BTC", "quote_asset": "USDT", "limits": ["0.0100"]},
            }
        ],
    }
    canonical = json.dumps(payload, sort_keys=True)
    tag = "database-snapshot:spot:"
    page._expected_tag = tag
    observers = []
    client.resultReceived.connect(lambda _tag, data: observers.append(data))
    try:
        client._finished(Reply(json.dumps(payload).encode()), tag, client._generation)
        assert page.table.rowCount() == 1 and page.snapshot is observers[0]
        page.table.selectRow(0)
        page.stream.setCheckState(Qt.CheckState.Checked)
        page.historical.setChecked(True)
        page.lookback.setValue(3)
        assert page._mode_dirty and page._lookback_dirty
        assert json.dumps(observers[0], sort_keys=True) == canonical
        assert page.snapshot["instruments"][0]["collecting_stream"] is False
        assert not client._pending
    finally:
        page.timer.stop()
        client.stop()
        page.close()
        page.deleteLater()
        QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
        qt_app.processEvents()
