"""Two-phase CA checkpoints consume only frozen, verified auxiliary frames."""

import json
import threading
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256

import pytest

from omega.analysis.catalog import validate_configuration
from omega.analysis.graph import FeatureGraph
from omega.application.market_frame_builder import MarketFrameBuilder
from omega.domain.errors import StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.auxiliary_codec import encode_observation
from omega.ports.auxiliary import StoredAuxiliary
from omega.runtime.analysis_execution import AnalysisExecution
from omega.runtime.analysis_manifest import AnalysisInputManifest, InputPolicy

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
DESTINATION = "a" * 64
IDENTITY = "1" * 32
NOW = 600000
START = NOW - 180000


def frozen(identity=IDENTITY, *, price_only=False):
    config = validate_configuration(
        {"moving_averages": [{"interval_id": 0, "ema_period": 2}]}
        if price_only
        else {"order_flow": [{"interval_id": 0, "analysis_code": "NES", "nSamples": 2}]}
    )
    digest = sha256(
        json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    return dict(
        analysis_id=identity,
        analysis_revision=1,
        template_id="2" * 32,
        template_revision=1,
        instrument=INSTRUMENT.key,
        configuration=config,
        configuration_hash=digest,
    )


def candle(opening):
    price = Decimal(100 + opening // 60000 % 4)
    return Candle(
        INSTRUMENT, opening, opening + 60000, price, price + 1, price - 1, price, Decimal(2)
    )


def observation(opening, *, quality="observed", available=NOW, value=10):
    return AuxiliaryInput(
        "trade_flow",
        (("AGGTRADE_NOTIONALBUY", Decimal(value)), ("AGGTRADE_NOTIONALSELL", Decimal(0))),
        opening,
        opening + 60000,
        opening + 60000,
        available,
        quality=quality,
        coverage=Decimal(1) if quality == "observed" else Decimal(0),
        source=INSTRUMENT.key + ":aggTrade",
    )


def stored(value, revision=1, previous=None):
    return StoredAuxiliary(
        INSTRUMENT,
        value,
        revision,
        sha256(encode_observation(INSTRUMENT, value)).hexdigest(),
        previous,
    )


class Source:
    def __init__(self):
        self.records = [stored(observation(opening)) for opening in range(START, NOW, 60000)]
        self.selections, self.pin_reads, self.anchors, self.closes = [], [], [], []
        self.missing, self.partial, self.block = None, None, None
        self.fail_close = False

    def check_destination(self, expected):
        assert expected == DESTINATION

    def read(self, keys, start, end, *, limit, as_of_ms=None):
        self.anchors.append((start, end, as_of_ms))
        return tuple(candle(opening) for opening in range(start, end, 60000))

    def get(self, instrument, channel, source, opening, *, as_of_ms):
        if opening == self.missing:
            return None
        eligible = [
            row
            for row in self.records
            if row.observation.window_start_ms == opening
            and row.observation.available_at_ms <= as_of_ms
        ]
        value = max(eligible, key=lambda row: row.revision, default=None)
        return (
            replace(
                value,
                observation=replace(value.observation, quality="partial", coverage=Decimal(0)),
            )
            if value and opening == self.partial
            else value
        )

    def get_auxiliary(self, instrument, channel, source, opening, *, as_of_ms):
        self.pin_reads.append((opening, as_of_ms))
        return self.get(instrument, channel, source, opening, as_of_ms=as_of_ms)

    def read_frames(
        self,
        instrument,
        start,
        end,
        *,
        as_of_ms,
        channels,
        maximum_metric_age_ms,
        retrospective_until_ms,
        limit,
    ):
        self.selections.append((start, end, as_of_ms, channels))
        if self.block:
            self.block[0].set()
            assert self.block[1].wait(3)
        builder = MarketFrameBuilder(self, maximum_metric_age_ms=maximum_metric_age_ms)
        return tuple(
            builder.prepare(
                candle(opening),
                as_of_ms=as_of_ms,
                channels=channels,
                retrospective=opening + 60000 <= retrospective_until_ms,
            )
            for opening in range(start, end, 60000)
        )

    def close(self):
        self.closes.append(threading.get_ident())
        if self.fail_close:
            raise StorageError("synthetic source close failed")


def policy(snapshot):
    return InputPolicy(
        DESTINATION, ("trade_flow",) if snapshot["configuration"].get("order_flow") else ()
    )


def engine(root, source=None, clock=None, **kwargs):
    source = source or Source()
    clock = clock or [NOW]
    value = AnalysisExecution(
        root,
        lambda: source,
        clock_ms=lambda: clock[0],
        input_policy_factory=policy,
        background=False,
        **kwargs,
    )
    return value, source, clock


def data(engine):
    return json.loads(engine._path(IDENTITY).read_text())["data"]


def test_schema2_uses_two_atomic_saves_and_exposes_only_consumed_outputs(tmp_path, monkeypatch):
    value, source, _ = engine(tmp_path)
    saves = []
    original = value._persist
    monkeypatch.setattr(
        value, "_persist", lambda job: (saves.append(deepcopy(job)), original(job))[1]
    )
    try:
        value.start(frozen(), warmup_minutes=3)
        assert value.run_once()
        assert [row["next_ms"] for row in saves] == [START, START, NOW]
        assert saves[1]["pending_input"] is not None and saves[1]["manifest_head"] is None
        assert saves[1]["last_candle"] is None
        assert (
            saves[2]["pending_input"] is None
            and saves[2]["manifest_head"] == saves[1]["pending_input"]
        )
        row = value.snapshot()["instances"][0]
        assert row["ready"] and row["outputs"]["0_NES_0_NES"] == 10
        assert row["input_quality"]["auxiliary_subscription"]
        assert row["input_quality"]["manifest_head"] and not row["input_quality"]["pending_input"]
        assert len(source.selections) == 1
    finally:
        value.close()


@pytest.mark.parametrize("restart_owner", [False, True])
def test_final_cp_failure_reuses_durable_pending_without_new_input_clock_selection(
    tmp_path, monkeypatch, restart_owner
):
    value, source, clock = engine(tmp_path)
    original = value._persist
    fail = True

    def final_failure(job):
        nonlocal fail
        if fail and job["next_ms"] > job["start_ms"] and job.get("pending_input") is None:
            fail = False
            raise StorageError("synthetic final CP failure")
        return original(job)

    monkeypatch.setattr(value, "_persist", final_failure)
    value.start(frozen(), warmup_minutes=3)
    assert not value.run_once()
    pending = data(value)["pending_input"]
    assert pending is not None and data(value)["next_ms"] == START
    assert value.results[IDENTITY] == {}
    first = source.records[0]
    source.records.append(
        stored(
            replace(
                first.observation,
                available_at_ms=NOW + 60000,
                values=(
                    ("AGGTRADE_NOTIONALBUY", Decimal(999)),
                    ("AGGTRADE_NOTIONALSELL", Decimal(0)),
                ),
            ),
            2,
            first.checksum,
        )
    )
    clock[0] += 60000
    if restart_owner:
        value.close()
        value, source, clock = engine(tmp_path, source, clock)
        assert value.snapshot()["instances"][0]["state"] == "stopped"
        assert (
            len(source.selections) == 1 and source.pin_reads == []
        )  # load only read manifest files.
    try:
        value.start(frozen(), warmup_minutes=3)
        assert value.run_once()
        assert data(value)["pending_input"] is None and data(value)["manifest_head"] == pending
        assert value.results[IDENTITY]["0_NES_0_NES"] == 10
        assert len(source.selections) == 1
        assert source.pin_reads and all(cutoff == NOW for _, cutoff in source.pin_reads)
        assert value.features[IDENTITY].last_input_frame.as_of_ms == NOW
    finally:
        value.close()


@pytest.mark.parametrize("quality", ["missing", "partial"])
def test_missing_or_partial_frames_never_advance_then_repair_uses_first_same_candle(
    tmp_path, quality
):
    value, source, _ = engine(tmp_path)
    setattr(source, quality, START + 60000)
    try:
        value.start(frozen(), warmup_minutes=3)
        before = value._path(IDENTITY).read_bytes()
        assert not value.run_once()
        assert value.jobs[IDENTITY]["state"] == "blocked"
        assert (
            value.jobs[IDENTITY]["next_ms"] == START
            and value.features[IDENTITY].last_candle is None
        )
        assert value._path(IDENTITY).read_bytes() == before and value.manifest is None
        setattr(source, quality, None)
        value.notify()
        assert value.run_once() and value.snapshot()["instances"][0]["ready"]
    finally:
        value.close()


def test_blocked_auxiliary_ca_does_not_prevent_an_independent_price_ca(tmp_path):
    value, source, _ = engine(tmp_path)
    source.missing = START
    try:
        value.start(frozen(), warmup_minutes=3)
        value.start(frozen("3" * 32, price_only=True), warmup_minutes=3)
        assert not value.run_once()
        assert value.run_once()
        assert value.jobs[IDENTITY]["next_ms"] == START
        assert value.jobs["3" * 32]["next_ms"] == NOW
        assert value.snapshot("3" * 32)["instances"][0]["ready"]
    finally:
        value.close()


@pytest.mark.parametrize("phase", ["manifest", "pending_cp"])
def test_stage_failures_leave_old_cursor_and_no_durable_pending(tmp_path, monkeypatch, phase):
    value, _, _ = engine(tmp_path)
    value.start(frozen(), warmup_minutes=3)
    before = value._path(IDENTITY).read_bytes()
    if phase == "manifest":
        monkeypatch.setattr(
            AnalysisInputManifest,
            "stage",
            lambda *a, **kw: (_ for _ in ()).throw(StorageError("fsync failed")),
        )
    else:
        original = value._persist

        def pending_failure(job):
            if job.get("pending_input") is not None:
                raise StorageError("pending CP failed")
            return original(job)

        monkeypatch.setattr(value, "_persist", pending_failure)
    try:
        assert not value.run_once()
        assert (
            value.jobs[IDENTITY]["next_ms"] == START
            and value.jobs[IDENTITY]["pending_input"] is None
        )
        assert value._path(IDENTITY).read_bytes() == before and value.results[IDENTITY] == {}
        if phase == "pending_cp":
            assert (
                sum(
                    len(path.read_bytes().splitlines())
                    for path in (tmp_path / "state/analysis-inputs/segments").glob("*.jsonl")
                )
                == 2
            )
    finally:
        value.close()


def test_policy_change_requires_restart_and_original_cp_is_archived_exactly(tmp_path):
    value, source, _ = engine(tmp_path)
    value.start(frozen(), warmup_minutes=3)
    assert value.run_once()
    original = value._path(IDENTITY).read_bytes()
    value.stop(IDENTITY)
    before = value._path(IDENTITY).read_bytes()
    value.input_policy_factory = lambda _: InputPolicy(
        DESTINATION, ("trade_flow",), maximum_metric_age_ms=100000
    )
    try:
        with pytest.raises(ValidationError, match="policy"):
            value.start(frozen())
        value.start(frozen(), warmup_minutes=3, restart=True)
        archives = list((tmp_path / "state/analysis-execution-history").glob("*.json"))
        assert len(archives) == 1 and archives[0].read_bytes() == before
        assert archives[0].read_bytes() != original
        assert data(value)["input_policy"]["maximum_metric_age_ms"] == 100000
        assert data(value)["manifest_head"] is None
    finally:
        value.close()


def test_history_failure_aborts_restart_without_replacing_current_cp(tmp_path, monkeypatch):
    value, _, _ = engine(tmp_path)
    value.start(frozen(), warmup_minutes=3)
    before = value._path(IDENTITY).read_bytes()
    monkeypatch.setattr(
        value,
        "_archive_checkpoint",
        lambda _: (_ for _ in ()).throw(StorageError("history fsync failed")),
    )
    try:
        with pytest.raises(StorageError):
            value.start(frozen(), warmup_minutes=3, restart=True)
        assert (
            value._path(IDENTITY).read_bytes() == before and value.jobs[IDENTITY]["generation"] == 0
        )
    finally:
        value.close()


def test_source_close_failure_still_releases_manifest_writer(tmp_path):
    value, source, _ = engine(tmp_path)
    value.start(frozen(), warmup_minutes=3)
    assert value.run_once()
    manifest = value.manifest
    source.fail_close = True
    value.close()
    assert manifest.closed and manifest.owner is None
    assert value.worker_error == "analysis_source_close_failed"
    replacement = AnalysisInputManifest(tmp_path)
    try:
        replacement._writer()
    finally:
        replacement.close()


def test_missing_manifest_on_load_preserves_checkpoint_and_queries_no_source(tmp_path):
    value, source, _ = engine(tmp_path)
    value.start(frozen(), warmup_minutes=3)
    assert value.run_once()
    value.close()
    path = value._path(IDENTITY)
    before = path.read_bytes()
    segment = next((tmp_path / "state/analysis-inputs/segments").glob("*.jsonl"))
    segment.rename(segment.with_suffix(".preserved"))
    calls = []
    restarted = AnalysisExecution(
        tmp_path,
        lambda: calls.append(1) or source,
        input_policy_factory=policy,
        clock_ms=lambda: NOW,
        background=False,
    )
    try:
        assert IDENTITY in restarted.load_errors and calls == []
        assert path.read_bytes() == before
    finally:
        restarted.close()


def test_legacy_auxiliary_checkpoint_requires_explicit_upgrade_and_is_preserved(tmp_path):
    source = Source()
    legacy = AnalysisExecution(tmp_path, lambda: source, clock_ms=lambda: NOW, background=False)
    legacy.start(frozen(), warmup_minutes=3)
    assert legacy.run_once() and data(legacy)["schema_version"] == 1
    legacy.close()
    original = legacy._path(IDENTITY).read_bytes()
    value, _, _ = engine(tmp_path, source)
    try:
        with pytest.raises(ValidationError, match="policy"):
            value.start(frozen())
        value.start(frozen(), warmup_minutes=3, restart=True)
        assert (
            next((tmp_path / "state/analysis-execution-history").glob("*.json")).read_bytes()
            == original
        )
        assert value.run_once() and value.snapshot()["instances"][0]["ready"]
        assert data(value)["schema_version"] == 2
    finally:
        value.close()


def test_nondefault_native_suffix_is_rejected_without_writing_a_checkpoint(tmp_path):
    value, _, _ = engine(tmp_path)
    value.input_policy_factory = lambda _: InputPolicy(
        DESTINATION, ("metric",), open_interest_source_suffix="another_oi"
    )
    try:
        with pytest.raises(ValidationError, match="custom metric"):
            value.start(frozen())
        assert value.jobs == {} and not value._path(IDENTITY).exists()
    finally:
        value.close()


@pytest.mark.parametrize(
    "damage", ["graph_frame", "same_generation_execution", "manifest_frame_timestamp"]
)
def test_valid_checksum_input_metadata_damage_is_rejected_on_load_without_db(tmp_path, damage):
    value, source, _ = engine(tmp_path)
    value.start(frozen(), warmup_minutes=3)
    assert value.run_once()
    job = deepcopy(value.jobs[IDENTITY])
    if damage == "graph_frame":
        graph = deepcopy(value.features[IDENTITY])
        graph._last_frame = replace(graph.last_input_frame, as_of_ms=NOW + 1)
        job["checkpoint"] = graph.checkpoint()
    elif damage == "same_generation_execution":
        job["execution_id"] = "f" * 32
    else:
        head = value.manifest.read(job["manifest_head"])
        chunk = value.manifest.read(head["source"])
        chunk["frames"][0]["as_of_ms"] = True
        head["source"] = value.manifest._append(chunk).document()
        job["manifest_head"] = value.manifest._append(head).document()
    value._persist(job)
    before = value._path(IDENTITY).read_bytes()
    value.close()
    calls = []
    restarted = AnalysisExecution(
        tmp_path,
        lambda: calls.append(1) or source,
        input_policy_factory=policy,
        clock_ms=lambda: NOW,
        background=False,
    )
    try:
        assert IDENTITY in restarted.load_errors and calls == []
        assert restarted._path(IDENTITY).read_bytes() == before
    finally:
        restarted.close()


def test_background_stop_discards_late_source_selection_without_writing_manifest(tmp_path):
    entered, release = threading.Event(), threading.Event()
    source = Source()
    source.block = (entered, release)
    value = AnalysisExecution(
        tmp_path, lambda: source, clock_ms=lambda: NOW, input_policy_factory=policy
    )
    try:
        value.start(frozen(), warmup_minutes=3)
        assert entered.wait(2)
        value.stop(IDENTITY)
        stopped = value._path(IDENTITY).read_bytes()
        release.set()
        value.close()
        assert value._path(IDENTITY).read_bytes() == stopped
        assert data(value)["pending_input"] is None and data(value)["next_ms"] == START
        assert not (tmp_path / "state/analysis-inputs/segments").exists()
    finally:
        release.set()
        value.close()


def test_stop_after_durable_stage_preserves_pending_for_manual_replay(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    source = Source()
    value = AnalysisExecution(
        tmp_path, lambda: source, clock_ms=lambda: NOW, input_policy_factory=policy
    )
    original = FeatureGraph.checkpoint
    blocked = False

    def pause_final_snapshot(graph):
        nonlocal blocked
        if (
            not blocked
            and graph.last_candle is not None
            and value.jobs.get(IDENTITY, {}).get("pending_input")
        ):
            blocked = True
            entered.set()
            assert release.wait(3)
        return original(graph)

    monkeypatch.setattr(FeatureGraph, "checkpoint", pause_final_snapshot)
    try:
        value.start(frozen(), warmup_minutes=3)
        assert entered.wait(2)
        pending = data(value)["pending_input"]
        assert pending is not None and data(value)["next_ms"] == START
        value.stop(IDENTITY)
        stopped = value._path(IDENTITY).read_bytes()
        release.set()
        value.close()
        assert value._path(IDENTITY).read_bytes() == stopped
    finally:
        release.set()
        value.close()
    resumed, _, _ = engine(tmp_path, source)
    try:
        resumed.start(frozen(), warmup_minutes=3)
        assert resumed.run_once()
        assert data(resumed)["manifest_head"] == pending and data(resumed)["pending_input"] is None
        assert len(source.selections) == 1 and resumed.snapshot()["instances"][0]["ready"]
    finally:
        resumed.close()


def test_source_destination_change_blocks_before_selection_and_cp_mutation(tmp_path, monkeypatch):
    value, source, _ = engine(tmp_path)
    monkeypatch.setattr(
        source,
        "check_destination",
        lambda _: (_ for _ in ()).throw(ValidationError("destination changed")),
    )
    try:
        value.start(frozen(), warmup_minutes=3)
        before = value._path(IDENTITY).read_bytes()
        assert not value.run_once()
        assert value.jobs[IDENTITY]["state"] == "blocked"
        assert source.selections == source.anchors == source.pin_reads == []
        assert value._path(IDENTITY).read_bytes() == before
    finally:
        value.close()
