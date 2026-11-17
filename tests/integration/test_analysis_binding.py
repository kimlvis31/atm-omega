"""Owner commands bind immutable analysis snapshots and protect their references."""

import pytest

from omega.runtime.app import build_core


@pytest.fixture
def core(tmp_path):
    result = build_core(tmp_path)
    yield result
    result.close()


def command(core, identity, kind, payload):
    return core.request(
        "POST", "/api/commands", core.token, {"id": identity, "kind": kind, "payload": payload}
    )


def configured_ca(core):
    result = command(
        core,
        "template",
        "analysis.template.create",
        {
            "expected_revision": 0,
            "name": "BASE",
            "configuration": {
                "moving_averages": [{"interval_id": 0, "ema_period": 4}],
                "momentum": [],
            },
        },
    )
    assert result["status"] == "completed", result
    template = result["result"]["item"]
    result = command(
        core,
        "ca",
        "analysis.instance.create",
        {
            "expected_revision": 1,
            "name": "BTC_BASE",
            "instrument": "binance:spot:BTCUSDT",
            "template_id": template["id"],
        },
    )
    assert result["status"] == "completed", result
    return template, result["result"]["item"]


def attach(core, ca):
    assert (
        command(core, "account", "paper.account.create", {"account_id": "a"})["status"]
        == "completed"
    )
    result = command(
        core,
        "strategy",
        "paper.strategy.add",
        {
            "instance": "q",
            "account_id": "a",
            "module_id": "target_quantity",
            "instrument": "binance:spot:BTCUSDT",
            "budget": "1000",
            "configuration": {"quantity": "0.01"},
            "analysis_id": ca["id"],
            "analysis_revision": ca["revision"],
        },
    )
    assert result["status"] == "completed", result


def test_saved_ca_binds_snapshot_and_cannot_be_deleted_while_strategy_references_it(core):
    _, ca = configured_ca(core)
    attach(core, ca)
    saved = core.paper.detail("q")["analysis_snapshot"]
    assert saved["analysis_id"] == ca["id"] and saved["configuration"] == ca["configuration"]
    result = command(
        core, "delete-bound", "analysis.instance.delete", {"id": ca["id"], "expected_revision": 2}
    )
    assert result["status"] == "rejected"
    assert not core.control.snapshot()["incidents"]
    assert (
        command(core, "remove", "paper.strategy.remove", {"instance": "q"})["status"] == "completed"
    )
    result = command(
        core, "delete-unbound", "analysis.instance.delete", {"id": ca["id"], "expected_revision": 2}
    )
    assert result["status"] == "completed"


def test_template_edit_and_explicit_ca_rebase_do_not_mutate_attached_strategy(core):
    template, ca = configured_ca(core)
    attach(core, ca)
    before = core.paper.detail("q")["analysis_snapshot"]
    updated = command(
        core,
        "change",
        "analysis.template.update",
        {
            "id": template["id"],
            "expected_revision": 2,
            "configuration": {
                "moving_averages": [{"interval_id": 0, "ema_period": 9}],
                "momentum": [],
            },
        },
    )
    assert updated["status"] == "completed", updated
    rebased = command(
        core,
        "rebase",
        "analysis.instance.rebase",
        {"id": ca["id"], "expected_revision": 3, "template_id": template["id"]},
    )
    assert rebased["status"] == "completed", rebased
    assert core.paper.detail("q")["analysis_snapshot"] == before


def test_library_operation_replay_has_one_receipt_and_invalid_revision_is_known(core):
    template, _ = configured_ca(core)
    payload = {"id": template["id"], "name": "DUPLICATE", "expected_revision": 2}
    first = command(core, "clone", "analysis.template.clone", payload)
    assert command(core, "clone", "analysis.template.clone", payload) == first
    snapshot = core.request("GET", "/api/analysis-library", core.token)
    assert len(snapshot["templates"]) == 2
    assert (
        command(core, "stale", "analysis.template.clone", payload | {"name": "STALE"})["status"]
        == "rejected"
    )
    assert not core.control.snapshot()["incidents"]


def test_leverage_startup_without_authorized_provider_does_not_contact_an_account(core):
    assert core.leverage_reference.provider is None
    core.leverage_reference.start()
    result = core.request("GET", "/api/reference/leverage", core.token)
    assert result["status"]["state"] == "auth_required"
    assert result["reference"] is None and result["status"]["fetched_at_ms"] is None
