"""Finish requested Qt deletions in GUI tests without a running event loop."""

import sys

import pytest


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_teardown(item, nextitem):
    yield
    # Do not initialize Qt or create an application during fixture cleanup.
    core = sys.modules.get("PySide6.QtCore")
    if core is None:
        return
    app = core.QCoreApplication.instance()
    if app is None:
        return
    assert core.QThread.currentThread() == app.thread(), "GUI teardown requires the app thread"
    # processEvents alone does not deliver DeferredDelete without an event loop.
    # Complete existing deleteLater requests; do not create new deletion work.
    core.QCoreApplication.sendPostedEvents(None, core.QEvent.Type.DeferredDelete)
