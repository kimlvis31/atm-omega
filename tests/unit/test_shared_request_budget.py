"""Actual OS locks and independent budget objects; no exchange requests."""

import json
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from omega.domain.errors import RateLimited, StorageError, ValidationError
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.shared_budget import SharedRequestBudget
from omega.infrastructure.binance.transport import HttpResponse


def test_reservation_header_and_cooldown_are_shared_across_restart(tmp_path):
    path = tmp_path / "budget.json"
    windows = (Window("ip", 4, 60),)
    first = SharedRequestBudget(path, windows, clock=lambda: 61)
    second = SharedRequestBudget(path, windows, clock=lambda: 61)
    first.reserve({"ip": 2})
    second.reserve({"ip": 1})
    first.observe_usage("ip", 60, 4)
    with pytest.raises(RateLimited) as error:
        second.reserve({"ip": 1})
    assert error.value.retry_after == 59
    second.cooldown(120)
    restarted = SharedRequestBudget(path, windows, clock=lambda: 120)
    with pytest.raises(RateLimited) as error:
        restarted.reserve({"ip": 1})
    assert error.value.retry_after == 61
    expired = SharedRequestBudget(path, windows, clock=lambda: 182)
    expired.reserve({"ip": 4})
    assert json.loads(path.read_text())["used"] == [["ip", 60, 3, 4]]


def test_backward_clock_never_reopens_an_old_window_after_forward_jump(tmp_path):
    path, now = tmp_path / "budget.json", [120]
    budget = SharedRequestBudget(path, (Window("ip", 2, 60),), clock=lambda: now[0])
    budget.reserve({"ip": 2})
    now[0] = 10
    with pytest.raises(RateLimited) as error:
        budget.reserve({"ip": 1})
    assert error.value.retry_after == 170
    now[0] = 179
    with pytest.raises(RateLimited):
        budget.reserve({"ip": 1})
    now[0] = 180
    budget.reserve({"ip": 2})


def test_response_header_across_minute_boundary_prunes_the_old_window(tmp_path):
    path, now = tmp_path / "budget.json", [59.9]
    windows = (Window("ip", 4, 60),)
    budget = SharedRequestBudget(path, windows, clock=lambda: now[0])
    budget.reserve({"ip": 1})
    now[0] = 60.1
    budget.observe_usage("ip", 60, 2)
    assert json.loads(path.read_text())["used"] == [["ip", 60, 1, 2]]
    fresh = SharedRequestBudget(path, windows, clock=lambda: now[0])
    fresh.reserve({"ip": 2})
    with pytest.raises(RateLimited):
        fresh.reserve({"ip": 1})


def test_boolean_schema_version_does_not_bypass_state_validation(tmp_path):
    path = tmp_path / "budget.json"
    budget = SharedRequestBudget(path, clock=lambda: 120)
    budget.reserve({"binance-ip": 1})
    state = json.loads(path.read_text())
    state["schema_version"] = True
    path.write_text(json.dumps(state))
    with pytest.raises(StorageError):
        budget.reserve({"binance-ip": 1})


@pytest.mark.parametrize(
    "content",
    ["{", "null", "[]", '{"schema_version":1}', "x" * 65537],
    ids=["truncated", "null", "array", "missing-fields", "oversized"],
)
def test_corrupt_state_is_preserved_and_blocks_requests(tmp_path, content):
    path = tmp_path / "budget.json"
    path.write_text(content)
    budget = SharedRequestBudget(path, clock=lambda: 120)
    with pytest.raises(StorageError):
        budget.reserve({"binance-ip": 1})
    assert path.read_text() == content


def test_storage_failure_never_admits_a_request_and_releases_lock(tmp_path, monkeypatch):
    from omega.infrastructure.binance import shared_budget

    path = tmp_path / "budget.json"
    budget = SharedRequestBudget(path, clock=lambda: 120)
    original = shared_budget.replace_atomic
    monkeypatch.setattr(
        shared_budget, "replace_atomic", lambda *_: (_ for _ in ()).throw(OSError())
    )
    with pytest.raises(StorageError):
        budget.reserve({"binance-ip": 1})
    assert not path.exists()
    assert not list(tmp_path.glob(".budget-*"))
    monkeypatch.setattr(shared_budget, "replace_atomic", original)
    budget.reserve({"binance-ip": 1})


def test_actual_os_lock_is_nonblocking_and_other_thread_has_no_partial_reservation(tmp_path):
    first = SharedRequestBudget(tmp_path / "budget.json", clock=lambda: 120)
    second = SharedRequestBudget(first.path, clock=lambda: 120)
    errors = []
    with first._locked():

        def attempt():
            try:
                second.reserve({"binance-ip": 1})
            except RateLimited as exc:
                errors.append(exc.retry_after)

        thread = threading.Thread(target=attempt)
        thread.start()
        thread.join(1)
        assert not thread.is_alive() and errors == [0.05]
        assert not first.path.exists()
    second.reserve({"binance-ip": 1})


def test_independent_processes_share_spent_reservations_without_a_daemon(tmp_path):
    path = tmp_path / "budget.json"
    script = """
import sys
import time
from pathlib import Path
from omega.infrastructure.binance.shared_budget import SharedRequestBudget
from omega.infrastructure.binance.budget import Window
from omega.domain.errors import RateLimited
budget = SharedRequestBudget(Path(sys.argv[1]), (Window('ip', 2, 60),), clock=lambda: 120)
for _ in range(100):
    try:
        budget.reserve({'ip': 1})
    except RateLimited as exc:
        if exc.retry_after != 0.05:
            print('blocked')
            break
        time.sleep(0.01)
    else:
        print('admitted')
        break
else:
    raise RuntimeError('fixture contention did not settle')
"""

    def run(_):
        return subprocess.run(
            [sys.executable, "-I", "-c", script, str(path)],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        ).stdout.strip()

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(run, range(3)))
    assert sorted(results) == ["admitted", "admitted", "blocked"]


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_readonly_gateway_failure_is_retryable_but_a_mutation_stays_unknown(status):
    from omega.domain.errors import OrderOutcomeUnknown, TransportError

    class Transport:
        def send(self, request):
            return HttpResponse(status, b"gateway error")

    budget = RequestBudget((Window("binance-ip", 600, 60),), clock=lambda: 120)
    client = BinanceClient(
        "https://api.binance.com", Transport(), budget=budget, enable_trading=True
    )
    with pytest.raises(TransportError):
        client.request("GET", "/api/v3/time")
    with pytest.raises(OrderOutcomeUnknown):
        client.request("POST", "/api/v3/order")


@pytest.mark.parametrize("status", [418, 429])
@pytest.mark.parametrize(
    "retry,expected", [("30", 30), (None, 60), ("nan", 60), ("inf", 60), ("-5", 60), ("bad", 60)]
)
def test_html_rate_limit_response_sets_shared_cooldown_before_json_decode(status, retry, expected):
    class Transport:
        calls = 0

        def send(self, request):
            self.calls += 1
            return HttpResponse(
                status, b"<html>Rate limit</html>", {} if retry is None else {"Retry-After": retry}
            )

    transport = Transport()
    budget = RequestBudget((Window("binance-ip", 600, 60),), clock=lambda: 120)
    client = BinanceClient("https://api.binance.com", transport, budget=budget)
    with pytest.raises(RateLimited) as error:
        client.request("GET", "/api/v3/time")
    assert error.value.retry_after == expected
    with pytest.raises(RateLimited):
        client.request("GET", "/api/v3/time")
    assert transport.calls == 1


@pytest.mark.parametrize("cost", [True, -1, 1.1, float("nan")])
def test_invalid_weights_cannot_disable_admission(cost):
    budget = RequestBudget((Window("ip", 1, 60),), clock=lambda: 120)
    with pytest.raises(ValidationError):
        budget.reserve({"ip": cost})
    budget.reserve({"ip": 1})
