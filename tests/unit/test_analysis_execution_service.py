"""Owner API binds saved CA revisions to actual calculation execution."""

from decimal import Decimal

import pytest

from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.analysis_execution import AnalysisExecution
from omega.runtime.analysis_library import AnalysisLibrary
from omega.runtime.service import CoreService

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


class Source:
    def __init__(self):
        self.calls = []

    def read(self, keys, start_ms, end_ms, *, limit):
        self.calls.append((keys, start_ms, end_ms, limit))
        return tuple(
            Candle(
                INSTRUMENT,
                opened,
                opened + 60000,
                Decimal(10),
                Decimal(12),
                Decimal(9),
                Decimal(11),
                Decimal(2),
            )
            for opened in range(start_ms, end_ms, 60000)
        )

    def close(self):
        pass


@pytest.fixture
def service(tmp_path):
    core = CoreService(tmp_path)
    source = Source()
    core.analysis_execution = AnalysisExecution(
        tmp_path, lambda: source, clock_ms=lambda: 600000, background=False
    )
    core.analysis_library = AnalysisLibrary(
        tmp_path / "state/analysis-library.json",
        references_provider=lambda: [
            {"analysis_id": row["analysis_id"], "strategy_id": row["execution_id"]}
            for row in core.analysis_execution.snapshot()["instances"]
            if row["state"] not in {"stopped", "error"}
        ],
    )
    yield core, source
    core.close()


def command(core, kind, payload, identifier):
    return core.request(
        "POST", "/api/commands", core.token, {"id": identifier, "kind": kind, "payload": payload}
    )


def create(core):
    result = command(
        core,
        "analysis.template.create",
        {
            "expected_revision": 0,
            "name": "CAC",
            "configuration": {
                "moving_averages": [{"interval_id": 0, "ema_period": 2, "mmacd_periods": []}]
            },
        },
        "template",
    )
    template = result["result"]["item"]
    result = command(
        core,
        "analysis.instance.create",
        {
            "expected_revision": 1,
            "name": "CA",
            "instrument": INSTRUMENT.key,
            "template_id": template["id"],
        },
        "instance",
    )
    return result["result"]["item"]


def start(core, ca, identifier="start", **changes):
    return command(
        core,
        "analysis.execution.start",
        {
            "analysis_id": ca["id"],
            "expected_revision": ca["revision"],
            "warmup_minutes": 3,
            **changes,
        },
        identifier,
    )


def test_saved_unstarted_ca_has_empty_execution_and_real_capability(service):
    core, source = service
    ca = create(core)
    result = core.request("GET", f"/api/analysis-execution?analysis_id={ca['id']}", core.token)
    assert result["instances"] == []
    assert result["core_epoch"] == core.state()["core"]["epoch"]
    assert core.request("GET", "/api/analysis-library", core.token)["capabilities"][
        "execution_subscription"
    ]
    assert source.calls == []


def test_start_is_idempotent_and_actual_calculation_is_independent_of_strategy(service):
    core, source = service
    ca = create(core)
    first = start(core, ca)
    assert first["status"] == "completed"
    assert start(core, ca) == first
    assert source.calls == []
    core.analysis_execution.run_once()
    result = core.request("GET", f"/api/analysis-execution?analysis_id={ca['id']}", core.token)
    row = result["instances"][0]
    assert row["ready"] and row["state"] == "running"
    assert row["outputs"]["0_EMA_0_EMA"] == 11.0
    assert core.state()["strategies"] == [] and len(source.calls) == 1


def test_stale_revision_and_untrusted_client_instrument_are_rejected(service):
    core, source = service
    ca = create(core)
    for index, changes in enumerate(
        (
            {"expected_revision": 0},
            {"expected_revision": True},
            {"instrument": "binance:spot:ETHUSDT"},
            {"restart": 1},
        )
    ):
        assert start(core, ca, f"invalid-{index}", **changes)["status"] == "rejected"
    assert core.analysis_execution.snapshot()["instances"] == [] and source.calls == []


def test_old_execution_cannot_stop_replacement_and_active_ca_cannot_be_deleted(service):
    core, _ = service
    ca = create(core)
    old = start(core, ca)["result"]
    new = start(core, ca, "restart", restart=True)["result"]
    assert old["execution_id"] != new["execution_id"]
    assert (
        command(
            core,
            "analysis.execution.stop",
            {"analysis_id": ca["id"], "execution_id": None},
            "missing-stop-fence",
        )["status"]
        == "rejected"
    )
    assert (
        command(
            core,
            "analysis.execution.stop",
            {"analysis_id": ca["id"], "execution_id": old["execution_id"]},
            "stale-stop",
        )["status"]
        == "rejected"
    )
    assert (
        command(
            core,
            "analysis.instance.delete",
            {"id": ca["id"], "expected_revision": 2},
            "delete-active",
        )["status"]
        == "rejected"
    )
    assert (
        command(
            core,
            "analysis.execution.stop",
            {"analysis_id": ca["id"], "execution_id": new["execution_id"]},
            "stop",
        )["status"]
        == "completed"
    )
    assert (
        command(
            core,
            "analysis.instance.delete",
            {"id": ca["id"], "expected_revision": 2},
            "delete-stopped",
        )["status"]
        == "completed"
    )


@pytest.mark.parametrize(
    "query", ["analysis_id=", "analysis_id=" + "a" * 32, "analysis_id=a&analysis_id=b", "unknown=x"]
)
def test_query_rejects_unknown_or_ambiguous_ca(service, query):
    core, _ = service
    with pytest.raises(ValidationError):
        core.request("GET", "/api/analysis-execution?" + query, core.token)


def test_mobile_device_cannot_read_or_start_analysis(service):
    core, _ = service
    ca = create(core)
    pairing = core.control.create_pairing("phone")
    device = core.request("POST", "/api/pair", "", {"code": pairing["code"], "name": "phone"})
    with pytest.raises(PermissionError):
        core.request("GET", "/api/analysis-execution", device["token"])
    with pytest.raises(PermissionError):
        core.request(
            "POST",
            "/api/commands",
            device["token"],
            {
                "id": "phone-start",
                "kind": "analysis.execution.start",
                "payload": {"analysis_id": ca["id"], "expected_revision": 1},
            },
        )
