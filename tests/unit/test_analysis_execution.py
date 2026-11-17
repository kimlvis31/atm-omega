"""Independent CA execution over synthetic committed-minute sources, without DB or orders."""

import json
import threading
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256

import pytest

from omega.analysis.catalog import configured_output_fields, create_features, validate_configuration
from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.analysis_execution import AnalysisExecution

MINUTE = 60000
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def test_seeded_ivp_no_peak_outputs_are_nullable_but_preseed_is_not_ready(tmp_path):
    pytest.importorskip("scipy")

    class ZeroProfile(Source):
        def read(self, *args, **kwargs):
            return tuple(replace(row, volume=Decimal(0)) for row in super().read(*args, **kwargs))

    clock = [10 * MINUTE]
    plan = frozen({"volume_profile": [{"interval_id": 0, "nSamples": 3}]})
    engine = AnalysisExecution(tmp_path, ZeroProfile, clock_ms=lambda: clock[0], background=False)
    try:
        engine.start(plan, warmup_minutes=2)
        assert engine.run_once()
        row = engine.snapshot()["instances"][0]
        assert not row["ready"] and row["reason"] == "warming_indicators"
        assert not row["input_quality"]["calculation_ready"]
        clock[0] += MINUTE
        assert engine.run_once()
        row = engine.snapshot()["instances"][0]
        assert row["ready"] and row["input_quality"]["calculation_ready"]
        assert row["input_quality"]["missing_outputs"] == []
        assert len(row["input_quality"]["nullable_outputs"]) == 10
        assert all(value is None for field, value in row["outputs"].items() if "_IVP_" in field)
        expected = row["outputs"]
    finally:
        engine.close()
    restarted = AnalysisExecution(
        tmp_path, ZeroProfile, clock_ms=lambda: clock[0], background=False
    )
    try:
        assert restarted.snapshot()["instances"][0]["state"] == "stopped"
        restarted.start(plan)
        assert restarted.run_once()
        assert restarted.snapshot()["instances"][0]["ready"]
        assert restarted.snapshot()["instances"][0]["outputs"] == expected
    finally:
        restarted.close()


def test_unsupported_auxiliary_subscription_never_reports_ready_from_seeded_ohlcv(tmp_path):
    plan = frozen({"order_flow": [{"interval_id": 0, "analysis_code": "WOI", "nSamples": 2}]})
    engine = AnalysisExecution(tmp_path, Source, clock_ms=lambda: 10 * MINUTE, background=False)
    try:
        engine.start(plan, warmup_minutes=3)
        assert engine.run_once()
        row = engine.snapshot()["instances"][0]
        assert row["state"] == "running" and not row["ready"]
        assert row["reason"] == "inputs_not_ready" and not row["input_quality"]["input_ready"]
        assert not row["input_quality"]["auxiliary_subscription"]
    finally:
        engine.close()


def frozen(configuration=None, *, identity="a" * 32, revision=1):
    config = validate_configuration(
        configuration or {"moving_averages": [{"interval_id": 0, "ema_period": 3}]}
    )
    payload = json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return {
        "analysis_id": identity,
        "analysis_revision": revision,
        "template_id": "b" * 32,
        "template_revision": 1,
        "instrument": INSTRUMENT.key,
        "configuration": config,
        "configuration_hash": sha256(payload).hexdigest(),
    }


def candle(index):
    price = Decimal(100 + index % 5)
    return Candle(
        INSTRUMENT,
        index * MINUTE,
        (index + 1) * MINUTE,
        price,
        price + 1,
        price - 1,
        price,
        Decimal(10),
    )


class Source:
    def __init__(self, *, gap=None, block=None):
        self.calls, self.closed, self.gap, self.block = [], [], gap, block
        self.changed = False

    def read(self, keys, low, high, *, limit):
        self.calls.append((keys, low, high, limit, threading.get_ident()))
        if self.block:
            self.block[0].set()
            assert self.block[1].wait(3)
        rows = tuple(
            candle(index) for index in range(low // MINUTE, high // MINUTE) if index != self.gap
        )
        if self.changed and rows:
            row = rows[0]
            rows = (
                Candle(
                    row.instrument,
                    row.open_time_ms,
                    row.close_time_ms,
                    Decimal(999),
                    Decimal(1000),
                    Decimal(998),
                    Decimal(999),
                    Decimal(1),
                ),
                *rows[1:],
            )
        return rows

    def close(self):
        self.closed.append(threading.get_ident())


def engine(tmp_path, source=None, *, now=None, mono=None, background=False):
    source = source or Source()
    now = now or [30 * MINUTE]
    mono = mono or [0.0]
    calls = []

    def factory():
        calls.append(threading.get_ident())
        return source

    instance = AnalysisExecution(
        tmp_path, factory, clock_ms=lambda: now[0], monotonic=lambda: mono[0], background=background
    )
    return instance, source, now, mono, calls


def row(instance):
    return instance.snapshot()["instances"][0]


def test_all_six_configured_providers_prepare_incrementally_and_match_independent_graph(tmp_path):
    config = {
        "moving_averages": [
            {"interval_id": 0, "ema_period": 3, "mmacd_periods": [2, 4], "signal_period": 2}
        ],
        "momentum": [
            {"interval_id": 0, "analysis_code": "MFI", "nSamples": 2},
            {"interval_id": 0, "analysis_code": "DMIxADX", "nSamples": 2},
        ],
        "swing": [{"interval_id": 0, "line_index": 0, "swing_range": 2}],
        "parabolic": [{"interval_id": 0, "line_index": 0}],
    }
    snapshot = frozen(config)
    instance, source, _, _, _ = engine(tmp_path)
    queued = instance.start(snapshot, warmup_minutes=20)
    assert queued["state"] == "queued" and source.calls == []
    assert instance.run_once()
    result = row(instance)
    expected = create_features(
        INSTRUMENT, configured_output_fields(snapshot["configuration"]), snapshot["configuration"]
    )
    output = {}
    for index in range(10, 30):
        output = expected.update(candle(index))
    assert result["outputs"] == output
    assert not result["ready"]  # This trajectory has no confirmed SWING pivot.
    assert all("SWING" in field for field in result["input_quality"]["missing_outputs"])
    assert result["warmup"]["processed_minutes"] == 20
    assert instance.features[snapshot["analysis_id"]].checkpoint() == expected.checkpoint()
    assert (
        not instance.run_once() and len(source.calls) == 1
    )  # Idle at latest closed target does no read.
    instance.close()


def test_higher_interval_preview_does_not_require_closed_true_to_be_ready(tmp_path):
    instance, _, _, _, _ = engine(tmp_path, now=[10 * MINUTE])
    instance.start(
        frozen({"moving_averages": [{"interval_id": 1, "ema_period": 2}]}), warmup_minutes=6
    )
    assert instance.run_once()
    result = row(instance)
    assert result["outputs"]["CLOSED_1"] is False and result["ready"]
    assert result["warmup"]["start_ms"] == 3 * MINUTE
    instance.close()


def test_insufficient_warmup_and_unconfirmed_swing_are_explicitly_not_ready(tmp_path):
    instance, _, _, _, _ = engine(tmp_path)
    instance.start(
        frozen({"moving_averages": [{"interval_id": 0, "ema_period": 100}]}), warmup_minutes=10
    )
    instance.run_once()
    assert row(instance)["state"] == "running" and not row(instance)["ready"]
    assert row(instance)["reason"] == "outputs_not_ready"
    instance.close()
    other, _, _, _, _ = engine(tmp_path / "swing")
    other.start(frozen({"swing": [{"interval_id": 0, "swing_range": 20}]}), warmup_minutes=2)
    other.run_once()
    assert not row(other)["ready"] and row(other)["input_quality"]["missing_outputs"]
    other.close()


def test_start_is_idempotent_frozen_and_changed_revision_requires_explicit_restart(tmp_path):
    instance, _, _, _, _ = engine(tmp_path)
    snapshot = frozen()
    original = instance.start(snapshot, warmup_minutes=10)
    assert instance.start(deepcopy(snapshot))["execution_id"] == original["execution_id"]
    snapshot["configuration"]["moving_averages"][0]["ema_period"] = 99
    assert row(instance)["configuration_hash"] == original["configuration_hash"]
    changed = frozen(revision=2)
    with pytest.raises(ValidationError, match="restart"):
        instance.start(changed)
    restarted = instance.start(changed, restart=True, warmup_minutes=5)
    with pytest.raises(ValidationError, match="stale"):
        instance.stop(changed["analysis_id"], expected_execution_id=original["execution_id"])
    assert row(instance)["execution_id"] == restarted["execution_id"]
    instance.close()


def test_restart_loads_stopped_and_manual_resume_checks_one_db_anchor_then_catches_up(tmp_path):
    snapshot = frozen()
    instance, _, _, _, _ = engine(tmp_path)
    instance.start(snapshot, warmup_minutes=10)
    instance.run_once()
    before = row(instance)["outputs"]
    instance.close()
    resumed, source, now, _, factory = engine(tmp_path)
    assert row(resumed)["state"] == "stopped" and not row(resumed)["ready"] and factory == []
    resumed.start(snapshot)
    assert resumed.run_once() and source.calls[0][1:3] == (29 * MINUTE, 30 * MINUTE)
    assert row(resumed)["ready"] and row(resumed)["outputs"] == before
    assert not resumed.run_once() and len(source.calls) == 1
    now[0] += MINUTE
    assert resumed.run_once() and row(resumed)["last_closed_ms"] == 31 * MINUTE
    resumed.close()


def test_gap_is_all_or_nothing_and_backoff_skips_repeated_db_reads_until_notified(tmp_path):
    instance, source, _, mono, _ = engine(tmp_path, Source(gap=25))
    instance.start(frozen(), warmup_minutes=10)
    assert not instance.run_once() and row(instance)["state"] == "blocked"
    assert row(instance)["last_closed_ms"] is None and not row(instance)["outputs"]
    assert not instance.run_once() and len(source.calls) == 1
    mono[0] = 0.5
    source.gap = None
    instance.notify()
    assert instance.run_once() and row(instance)["ready"]
    instance.close()


def test_changed_durable_anchor_after_resume_is_blocked_without_advancing_checkpoint(tmp_path):
    instance, source, _, _, _ = engine(tmp_path)
    snapshot = frozen()
    instance.start(snapshot, warmup_minutes=10)
    instance.run_once()
    instance.stop(snapshot["analysis_id"])
    instance.start(snapshot)
    source.changed = True
    assert not instance.run_once() and row(instance)["state"] == "blocked"
    assert row(instance)["last_closed_ms"] == 30 * MINUTE
    instance.close()


def test_stop_during_blocked_db_read_fences_its_late_return_and_keeps_control_responsive(tmp_path):
    entered, release = threading.Event(), threading.Event()
    instance, _, _, _, _ = engine(tmp_path, Source(block=(entered, release)))
    started = instance.start(frozen(), warmup_minutes=10)
    thread = threading.Thread(target=instance.run_once)
    thread.start()
    assert entered.wait(2)
    stopped = instance.stop(started["analysis_id"], expected_execution_id=started["execution_id"])
    assert stopped["state"] == "stopped" and stopped["last_closed_ms"] is None
    release.set()
    thread.join(3)
    assert not thread.is_alive() and row(instance)["outputs"] == {}
    instance.close()


def test_background_worker_owns_creation_reads_and_close_on_same_thread(tmp_path):
    instance, source, _, _, factories = engine(tmp_path, background=True)
    # Event-driven signal without repeatedly polling the GUI or sleeping.
    completed = threading.Event()
    original = instance._persist

    def persist(job):
        original(job)
        if job["next_ms"] == job["target_ms"]:
            completed.set()

    instance._persist = persist
    instance.start(frozen(), warmup_minutes=10)
    assert completed.wait(3)
    instance.close()
    assert len(factories) == len(source.closed) == 1
    assert factories[0] == source.calls[0][-1] == source.closed[0] != threading.get_ident()


def test_checkpoint_write_failure_does_not_publish_new_graph_or_cursor(tmp_path):
    instance, _, _, _, _ = engine(tmp_path)
    instance.start(frozen(), warmup_minutes=10)
    path = instance._path("a" * 32)
    before = path.read_bytes()
    instance._persist = lambda _job: (_ for _ in ()).throw(StorageError("synthetic write failure"))
    assert not instance.run_once()
    assert (
        row(instance)["state"] == "error"
        and row(instance)["reason"] == "checkpoint_persistence_unavailable"
    )
    assert row(instance)["last_closed_ms"] is None and path.read_bytes() == before
    instance.close()


def test_bad_checkpoint_is_preserved_and_other_cas_can_start_without_loading_it(tmp_path):
    directory = tmp_path / "state/analysis-executions"
    directory.mkdir(parents=True)
    path = directory / (("a" * 32) + ".json")
    path.write_bytes(b"{not json")
    instance, _, _, _, _ = engine(tmp_path)
    assert instance.snapshot()["load_errors"] == [
        {"analysis_id": "a" * 32, "reason": "invalid_execution_checkpoint_preserved"}
    ]
    with pytest.raises(ValidationError, match="invalid checkpoint"):
        instance.start(frozen())
    instance.start(frozen(identity="c" * 32), warmup_minutes=10)
    assert instance.run_once() and path.read_bytes() == b"{not json"
    instance.close()


def test_staleness_clears_readiness_and_results_are_detached_copies(tmp_path):
    instance, _, now, _, _ = engine(tmp_path)
    instance.start(frozen(), warmup_minutes=10)
    instance.run_once()
    result = instance.snapshot("a" * 32)
    result["instances"][0]["outputs"].clear()
    assert row(instance)["outputs"] and row(instance)["ready"]
    now[0] += 3 * MINUTE
    assert not row(instance)["ready"] and row(instance)["reason"] == "stale"
    instance.close()
    assert not row(instance)["ready"]


def test_fixed_warmup_uses_512_chunks_and_later_live_reads_only_incremental_tail(tmp_path):
    instance, source, now, _, _ = engine(tmp_path, now=[1200 * MINUTE])
    instance.start(frozen(), warmup_minutes=600)
    assert instance.run_once()
    first = row(instance)
    assert first["state"] == "preparing" and first["warmup"]["processed_minutes"] == 512
    assert first["last_closed_ms"] == 1112 * MINUTE
    now[0] = 1300 * MINUTE
    assert instance.run_once()
    assert row(instance)["warmup"]["target_ms"] == 1200 * MINUTE
    assert row(instance)["warmup"]["processed_minutes"] == 600
    assert instance.run_once() and row(instance)["last_closed_ms"] == 1216 * MINUTE
    assert [(call[2] - call[1]) // MINUTE for call in source.calls] == [512, 89, 17]
    instance.close()


def test_64_definition_ca_does_not_consume_ts_required_field_limit_or_allow_unknown_fields(
    tmp_path,
):
    config = {
        "swing": [
            {"interval_id": index // 10, "line_index": index % 10, "swing_range": 2}
            for index in range(64)
        ]
    }
    snapshot = frozen(config)
    fields = configured_output_fields(snapshot["configuration"])
    assert len(fields) > 256
    instance, source, _, _, _ = engine(tmp_path)
    instance.start(snapshot, warmup_minutes=1)
    assert source.calls == []
    configured = instance.features[snapshot["analysis_id"]]
    result = configured.update(candle(0))
    assert fields <= result.keys()
    with pytest.raises(ValidationError, match="does not provide"):
        create_features(INSTRUMENT, {"NONEXISTENT_OUTPUT"}, snapshot["configuration"])
    instance.close()


def test_corrupted_graph_cursor_even_with_rehashed_envelope_is_not_auto_recovered(tmp_path):
    instance, _, _, _, _ = engine(tmp_path)
    instance.start(frozen(), warmup_minutes=10)
    instance.run_once()
    instance.close()
    path = instance._path("a" * 32)
    envelope = json.loads(path.read_bytes())
    envelope["data"]["next_ms"] += MINUTE
    payload = json.dumps(
        envelope["data"], sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    envelope["sha256"] = sha256(payload).hexdigest()
    path.write_text(json.dumps(envelope), encoding="utf-8")
    saved = path.read_bytes()
    reloaded, _, _, _, _ = engine(tmp_path)
    assert reloaded.snapshot()["instances"] == [] and reloaded.snapshot()["load_errors"]
    assert path.read_bytes() == saved
    reloaded.close()
