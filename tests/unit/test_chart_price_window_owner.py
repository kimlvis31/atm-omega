"""The shared job owner preserves strict new request identity and scalar cache limits."""

import threading
from copy import deepcopy

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.chart_series import ChartSeriesService


def specification():
    return {
        "instrument": "binance:usdm:BTCUSDT",
        "start_ms": 0,
        "display_start_ms": 0,
        "end_ms": 600_000,
        "period_minutes": 1,
        "kind": "PRICE_WINDOWS",
        "definitions": [
            {"analysis_code": "SMA", "line_index": 0, "nSamples": 3, "price_precision": 8},
            {
                "analysis_code": "BOL",
                "line_index": 0,
                "nSamples": 3,
                "price_precision": 8,
                "MAType": "SMA",
                "bandWidth": 2,
            },
        ],
    }


def terminal(owner, identity):
    with owner.condition:
        assert owner.condition.wait_for(
            lambda: owner.jobs[identity]["state"] not in {"queued", "running"}, timeout=5
        )
    result = owner.status(identity)
    assert result["state"] == "complete", result
    return result


def test_reordered_definitions_reuse_one_job_but_native_numeric_type_is_part_of_identity():
    creations, closes = [], []

    class Source:
        def read(self, keys, start, end, *, limit, as_of_ms):
            return ()

        def close(self):
            closes.append(threading.get_ident())

    def factory():
        creations.append(threading.get_ident())
        return Source()

    owner = ChartSeriesService(factory, clock_ms=lambda: 660_000)
    spec = specification()
    try:
        owner.submit("one", spec)
        completed = terminal(owner, "one")
        reordered = deepcopy(spec)
        reordered["definitions"].reverse()
        assert owner.submit("one", reordered) == completed
        # Equal Python numeric values do not imply equal frozen JSON requests.
        changed = deepcopy(spec)
        changed["definitions"][1]["bandWidth"] = 2.0
        with pytest.raises(ValidationError, match="conflicts"):
            owner.submit("one", changed)
        assert owner.status("one") == completed
        assert creations == closes and len(creations) == 1
        assert creations[0] != threading.get_ident()
    finally:
        owner.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"start_ms": True},
        {"start_ms": 1},
        {"end_ms": 660_001},
        {"end_ms": 700_000_000},
        {"period_minutes": True},
        {"period_minutes": 2},
        {"instrument": "another:spot:BTCUSDT"},
        {"unrecognized": 1},
    ],
)
def test_common_admission_rejects_before_creating_a_source_or_worker(changes):
    creations = []
    owner = ChartSeriesService(lambda: creations.append("unexpected"), clock_ms=lambda: 660_000)
    try:
        with pytest.raises(ValidationError):
            owner.submit("rejected", specification() | changes)
        assert creations == [] and owner.thread is None and not owner.jobs
    finally:
        owner.close()


def test_new_kind_cache_charges_each_output_scalar_instead_of_one_per_bar():
    owner = ChartSeriesService(lambda: None, maximum_cached_rows=10_000)
    try:
        for index in range(4):
            owner.jobs[str(index)] = {
                "state": "complete",
                "result": {
                    "kind": "PRICE_WINDOWS",
                    "rows": [{"values": [0] * 30} for _ in range(100)],
                },
            }
        owner._expire_results()
        assert owner.jobs["0"] == {"state": "expired", "result": None}
        assert all(owner.jobs[str(index)]["state"] == "complete" for index in (1, 2, 3))
        assert (
            sum(
                len(row["values"])
                for job in owner.jobs.values()
                if job["result"] is not None
                for row in job["result"]["rows"]
            )
            == 9000
        )
    finally:
        owner.close()
