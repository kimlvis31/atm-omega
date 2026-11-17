"""Runtime must not silently drop an opt-in policy or report a legacy factory as costed."""

import time
from copy import deepcopy

import pytest

from omega.application.margin_cost import MarginCostPolicy
from omega.runtime.margin_trading import MarginTradingService

DOCUMENT = {
    "schema_version": 1,
    "model": "linear_usdt_ohlcv_cost_v1",
    "slippage_rate": "0.0100",
    "execution_price_bound_rate": "0.1",
    "mark_drift_bound_rate": "0.1",
    "entry_delay_ms": 1,
    "reduction_delay_ms": 0,
    "limit_price_policy": "adverse_then_cap_v1",
}


def await_value(read, check):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        value = read()
        if check(value):
            return value
        time.sleep(0.01)
    raise AssertionError(value)


class Factory:
    supports_cost_policy = True
    honors = True
    close_failure = False
    instances = []

    @staticmethod
    def prepare_configuration(*args, **kwargs):
        pass

    def __init__(self, root, account, strategies, start_ms, **options):
        self.account_id = account["account_id"]
        self.close_count = 0
        self.options = options
        type(self).instances.append(self)

    def snapshot(self):
        value = {
            "account_id": self.account_id,
            "state": "ready",
            "paused": True,
            "next_open_ms": 0,
            "pending_frame": False,
            "instruments": [],
        }
        if self.honors:
            policy = MarginCostPolicy.parse(self.options["cost_policy"])
            value["cost_policy"] = {
                "scope": "fixed_adverse_full_fill_delay_v1",
                "policy": policy.document(),
                "policy_sha256": policy.sha256,
                "venue_execution": False,
            }
        return value

    def close(self):
        self.close_count += 1
        if self.close_failure and self.close_count == 1:
            raise OSError("first cleanup acknowledgement missing")


def start(tmp_path, factory):
    def source():
        pytest.fail("configuration must not start a market source")

    runtime = MarginTradingService(
        tmp_path, None, source, core_epoch="core", session_factory=factory, clock_ms=lambda: 60000
    )
    runtime.start()
    await_value(runtime.snapshot, lambda v: v["state"] == "ready")
    return runtime


def configure(runtime, document=None):
    runtime.submit(
        "configure",
        "configure",
        {
            "account": {"account_id": "cost"},
            "strategies": [],
            "start_ms": 0,
            "brackets": [],
            "cost_policy": deepcopy(DOCUMENT if document is None else document),
        },
    )
    return await_value(
        lambda: runtime.status("configure"), lambda v: v["state"] not in {"queued", "running"}
    )


def test_cost_options_are_forwarded_without_enabling_nn_or_reformatting_raw_document(tmp_path):
    runtime = MarginTradingService(tmp_path, None, lambda: None, core_epoch="core")
    parameters = {"strategies": [], "cost_policy": DOCUMENT}
    assert runtime._session_options(parameters) == {"cost_policy": DOCUMENT}
    assert runtime._feature_options(parameters) == {}
    assert runtime._session_options({"strategies": []}) == {}
    assert runtime.snapshot()["capabilities"]["cost_policy"] is True
    assert (
        runtime.snapshot()["capabilities"]["cost_policy_scope"]
        == "fixed_adverse_full_fill_delay_v1"
    )


def test_supported_factory_confirms_actual_policy_before_configuration_success(tmp_path):
    class Supported(Factory):
        instances = []

    runtime = start(tmp_path, Supported)
    try:
        assert configure(runtime)["state"] == "completed"
        session = Supported.instances[0]
        assert session.options["cost_policy"] == DOCUMENT
        assert (
            "clock_ms" not in session.options and "external_feature_provider" not in session.options
        )
        assert (
            runtime.snapshot()["accounts"][0]["cost_policy"]["policy_sha256"]
            == MarginCostPolicy.parse(DOCUMENT).sha256
        )
    finally:
        runtime.close()
    assert session.close_count == 1


def test_unsupported_factory_rejects_before_creating_any_account_or_session(tmp_path):
    class Unsupported(Factory):
        supports_cost_policy = False
        instances = []

    runtime = start(tmp_path, Unsupported)
    try:
        assert runtime.snapshot()["capabilities"]["cost_policy"] is False
        assert configure(runtime)["state"] == "failed"
        assert runtime.data["accounts"] == {} and Unsupported.instances == []
    finally:
        runtime.close()


@pytest.mark.parametrize("cleanup_failure", [False, True])
def test_factory_that_ignores_policy_never_reports_success_and_retains_uncertain_cleanup(
    tmp_path,
    cleanup_failure,
):
    class Ignored(Factory):
        honors = False
        close_failure = cleanup_failure
        instances = []

    runtime = start(tmp_path, Ignored)
    try:
        assert configure(runtime)["state"] == "unknown"
        assert "cost" not in runtime.sessions
        if cleanup_failure:
            await_value(runtime.snapshot, lambda v: not v["worker_alive"])
            assert runtime._rejected_sessions == Ignored.instances
    finally:
        runtime.close()
    assert Ignored.instances[0].close_count == (2 if cleanup_failure else 1)


@pytest.mark.parametrize(
    "change",
    [
        {"entry_delay_ms": True},
        {"slippage_rate": 0.01},
        {"model": "unsupported"},
        {"reduction_delay_ms": 86400001},
    ],
)
def test_invalid_policy_is_rejected_before_factory_or_account_namespace(tmp_path, change):
    class Supported(Factory):
        instances = []

    runtime = start(tmp_path, Supported)
    try:
        assert configure(runtime, {**DOCUMENT, **change})["state"] == "failed"
        assert not Supported.instances and runtime.data["accounts"] == {}
        assert not (tmp_path / "state/margin-v2").exists()
    finally:
        runtime.close()
