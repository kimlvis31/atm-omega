"""The mobile URL draft uses revisioned settings and never silently normalizes."""

import importlib.util
from copy import deepcopy

import pytest
from test_runtime_settings import app as app
from test_runtime_settings import page as page

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


def with_mobile(page):
    state = deepcopy(page.current)
    state["mobile"] = {"https_origin": ""}
    page.update_settings(state)
    return page.editors["mobile", "https_origin"]


def test_mobile_origin_editor_posts_only_explicit_revisioned_change(page):
    editor = with_mobile(page)
    editor.setText("https://omega.private.example")
    page.apply()
    assert page.client.commands[-1][:2] == (
        "settings.patch",
        {
            "revision": 2,
            "changes": {
                "mobile": {"https_origin": "https://omega.private.example"},
            },
        },
    )


def test_invalid_mobile_origin_draft_stays_local(page):
    editor = with_mobile(page)
    editor.setText(" https://omega.private.example")
    with pytest.raises(ValueError):
        page.changes()
    page.apply()
    assert page.client.commands == [] and page.dirty


def test_clear_origin_is_an_explicit_disable_draft(page):
    with_mobile(page)
    state = deepcopy(page.current)
    state["mobile"]["https_origin"] = "https://omega.private.example"
    page.update_settings(state)
    page.editors["mobile", "https_origin"].clear()
    assert page.changes() == {"mobile": {"https_origin": ""}}
