"""Lifecycle requests bind account, native evidence and the current core epoch."""

from copy import deepcopy

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.margin_retirement_client import (
    history_job_url,
    history_load_body,
    history_load_url,
    retirement_body,
    retirement_response,
    retirement_url,
    validate_retired_history,
    validate_retirement_observation,
)
from omega.runtime.service import CoreService

SHA = "a" * 64


def ready(epoch="epoch", account="account"):
    return retirement_response(
        account,
        epoch,
        state="ready",
        observation_sha256=SHA,
        registered_parameters_sha256="b" * 64,
        session_config_hash="c" * 64,
        ledger_namespace="d" * 64,
        paused=True,
        open_positions=0,
        pending_orders=0,
        staged=0,
        source_pending=False,
        strategy_count=1,
        source_record_count=1,
        ledger_total=0,
    )


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": True},
        {"account_id": "other"},
        {"core_epoch": "old"},
        {"paused": False},
        {"open_positions": 1},
        {"pending_orders": True},
        {"staged": 1},
        {"source_pending": True},
        {"source_record_count": 0},
        {"strategy_count": None},
        {"observation_sha256": "A" * 64},
        {"phase": []},
        {"receipt_sha256": SHA},
    ],
)
def test_readiness_cannot_be_manufactured_from_stale_or_unconfirmed_fields(change):
    value = ready()
    value.update(change)
    with pytest.raises(ValidationError):
        validate_retirement_observation(value, account_id="account", core_epoch="epoch")


def test_unavailable_view_never_contains_a_retry_authorization():
    value = retirement_response("account", "epoch", reason_code="account_unavailable")
    assert value["observation_sha256"] is value["open_positions"] is None
    with pytest.raises(ValidationError):
        retirement_body("operation", "account", value["observation_sha256"], "epoch")


def test_wire_urls_and_bodies_preserve_exact_ids_and_hashes():
    assert retirement_url("account:one") == "/api/margin-trading/accounts/account%3Aone/retirement"
    assert history_job_url("read:one") == "/api/margin-trading/history/read%3Aone"
    body = retirement_body("retire:one", "account:one", SHA, "epoch")
    assert body["parameters"] == {"account_id": "account:one", "expected_observation_sha256": SHA}
    assert history_load_body("read", "account", SHA, "epoch")["expected_receipt_sha256"] == SHA


class Backend:
    def __init__(self, epoch):
        self.core_epoch, self.calls = epoch, []

    def inspect_retirement(self, account_id):
        self.calls.append(("inspect", account_id))
        return ready(self.core_epoch, account_id)

    def load_retired_history(self, identity, account_id, receipt):
        self.calls.append(("load", identity, account_id, receipt))
        return self._job(identity, account_id, receipt, "queued")

    def retired_history_status(self, identity):
        self.calls.append(("status", identity))
        return self._job(identity, "account", SHA, "completed")

    @staticmethod
    def _job(identity, account, receipt, state):
        return {
            "schema_version": 1,
            "scope": "virtual_margin_retired_history",
            "job_id": identity,
            "account_id": account,
            "receipt_sha256": receipt,
            "state": state,
            "reason_code": None,
            "cached": state == "completed",
            "ledger_total": 0 if state == "completed" else None,
        }

    def close(self):
        pass


@pytest.fixture
def core(tmp_path):
    value = CoreService(tmp_path)
    value.margin_trading = Backend(value.control.snapshot()["epoch"])
    yield value
    value.close()


def test_owner_routes_preflight_and_explicit_history_without_financial_dispatch(core):
    epoch = core.control.snapshot()["epoch"]
    observation = core.request("GET", retirement_url("account:one"), core.token)
    assert observation["state"] == "ready" and observation["core_epoch"] == epoch
    job = core.request(
        "POST", history_load_url(), core.token, history_load_body("read:one", "account", SHA, epoch)
    )
    assert validate_retired_history(job, identity="read:one", core_epoch=epoch)["state"] == "queued"
    result = core.request("GET", history_job_url("read:one"), core.token)
    assert result["state"] == "completed" and result["cached"]
    assert core.margin_trading.calls == [
        ("inspect", "account:one"),
        ("load", "read:one", "account", SHA),
        ("status", "read:one"),
    ]


@pytest.mark.parametrize("route", ["preflight", "load", "status"])
def test_an_old_backend_epoch_is_not_relabelled_as_the_new_owner(core, route):
    core.margin_trading.core_epoch = "old-core"
    method, path, body = (
        ("GET", retirement_url("account"), None)
        if route == "preflight"
        else (
            "POST",
            history_load_url(),
            history_load_body("read", "account", SHA, core.control.snapshot()["epoch"]),
        )
        if route == "load"
        else ("GET", history_job_url("read"), None)
    )
    with pytest.raises(ValidationError):
        core.request(method, path, core.token, body)
    assert core.margin_trading.calls == ([("inspect", "account")] if route == "preflight" else [])


def test_retirement_mutation_rejects_a_replaced_backend_before_dispatch(core):
    core.margin_trading.core_epoch = "old-core"
    body = retirement_body("retire", "account", SHA, core.control.snapshot()["epoch"])
    with pytest.raises(ValidationError, match="another core epoch"):
        core.request("POST", "/api/margin-trading", core.token, body)
    assert core.margin_trading.calls == []


@pytest.mark.parametrize(
    "method,path,body",
    [
        ("GET", "/api/margin-trading/accounts/a%2Fb/retirement", None),
        ("GET", "/api/margin-trading/accounts/account/retirement?x=1", None),
        ("GET", "/api/margin-trading/history/read/nested", None),
        ("GET", "/api/margin-trading/history/%ff", None),
        ("POST", "/api/margin-trading/history", {"id": "read"}),
    ],
)
def test_malformed_routes_do_not_start_background_archive_work(core, method, path, body):
    with pytest.raises(ValidationError):
        core.request(method, path, core.token, deepcopy(body))
    assert core.margin_trading.calls == []


def test_registered_read_only_device_cannot_retire_or_read_private_archive(core):
    pairing = core.control.create_pairing("phone", ["read", "pause"])
    device = core.control.pair(pairing["code"])
    epoch = core.control.snapshot()["epoch"]
    for method, path, body in (
        ("GET", retirement_url("account"), None),
        ("POST", "/api/margin-trading", retirement_body("retire", "account", SHA, epoch)),
        ("POST", history_load_url(), history_load_body("read", "account", SHA, epoch)),
        ("GET", history_job_url("read"), None),
    ):
        with pytest.raises(PermissionError):
            core.request(method, path, device["token"], body)
    assert core.margin_trading.calls == []
