"""Real graph, file manifest and paper owner; only observed market IO is faked."""

import json
import threading
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal

import pytest

from omega.analysis.catalog import create_features
from omega.application.market_frame_builder import MarketFrameBuilder
from omega.application.paper import PaperTradingService
from omega.application.paper_inputs import PaperInputBatch
from omega.application.paper_state import validate_state
from omega.domain.errors import StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame, market_frame_checksum
from omega.domain.models import Candle, Instrument, Market
from omega.domain.trading import close_position
from omega.infrastructure.auxiliary_codec import encode_observation, observation_checksum
from omega.ports.auxiliary import StoredAuxiliary
from omega.ports.modules import ModuleDescriptor
from omega.runtime.analysis_manifest import AnalysisInputManifest, InputPolicy
from omega.runtime.analysis_replay import replay
from omega.runtime.paper_feed import PaperFeedSupervisor
from omega.runtime.paper_inputs import PaperAnalysisSource
from omega.strategies.target_quantity.module import TargetQuantityModule

MINUTE = 60000
BTC = Instrument("binance", Market.USDM, "BTCUSDT")
FIELD = "0_NES_0_NES"
POLICY = InputPolicy("a" * 64, ("trade_flow",))


def bar(index, low="99"):
    return Candle(
        BTC,
        index * MINUTE,
        (index + 1) * MINUTE,
        Decimal(100),
        Decimal(101),
        Decimal(low),
        Decimal(100),
        Decimal(10),
    )


def flow(candle, buy="12", available=None):
    value = AuxiliaryInput(
        "trade_flow",
        (("AGGTRADE_NOTIONALBUY", Decimal(buy)), ("AGGTRADE_NOTIONALSELL", Decimal(2))),
        candle.open_time_ms,
        candle.close_time_ms,
        candle.close_time_ms,
        available or candle.close_time_ms,
        source=candle.instrument.key + ":aggTrade",
    )
    return StoredAuxiliary(
        candle.instrument,
        value,
        1,
        observation_checksum(encode_observation(candle.instrument, value)),
        None,
    )


class Source:
    def __init__(self, rows):
        self.rows = list(rows)
        self.pins = {c.open_time_ms: flow(c) for c in rows}
        self.calls = self.replays = 0
        self.fail = self.closed = False
        self.secondary = {}

    def read(self, keys, low, high, **kwargs):
        return tuple(
            c for c in self.rows if c.instrument.key in keys and low <= c.open_time_ms < high
        )

    def check_destination(self, value):
        assert value == POLICY.destination_id

    def get(self, instrument, channel, source, stamp, *, as_of_ms):
        value = (self.pins if instrument == BTC else self.secondary).get(stamp)
        return (
            value if value is not None and value.observation.available_at_ms <= as_of_ms else None
        )

    def get_auxiliary(self, *args, **kwargs):
        self.replays += 1
        return self.get(*args, **kwargs)

    def read_frames(
        self,
        instrument,
        low,
        high,
        *,
        as_of_ms,
        channels,
        maximum_metric_age_ms,
        retrospective_until_ms,
        limit,
    ):
        self.calls += 1
        if self.fail:
            raise StorageError("private connection details")
        builder = MarketFrameBuilder(self)
        return tuple(
            builder.prepare(
                c,
                as_of_ms=as_of_ms,
                channels=channels,
                retrospective=retrospective_until_ms is not None,
            )
            for c in self.rows
            if c.instrument == instrument and low <= c.open_time_ms < high
        )

    def close(self):
        self.closed = True


class Module(TargetQuantityModule):
    descriptor = ModuleDescriptor("nes_target", "1", frozenset({FIELD}), True)

    def evaluate(self, context):
        if context.position.quantity and context.closed_candle.low <= 90:
            return close_position(context, "local_test_stop")
        return super().evaluate(context)


def factory(_name, instrument, configuration):
    return Module(instrument, configuration)


def setup(tmp_path, *, persist=lambda _: None):
    paper = PaperTradingService(factory, create_features, persist=persist)
    paper.command("paper.account.create", {"account_id": "a", "balance": "10000", "fee_rate": "0"})
    paper.command(
        "paper.strategy.add",
        {
            "account_id": "a",
            "instance": "s",
            "instrument": BTC.key,
            "module_id": "nes_target",
            "configuration": {"quantity": "1"},
            "analysis": {"order_flow": [{"interval_id": 0, "analysis_code": "NES", "nSamples": 2}]},
            "budget": "1000",
        },
    )
    paper.feed_configure(True, start_ms=10 * MINUTE, warmup_minutes=2)
    now = [11 * MINUTE]
    source = Source([bar(i) for i in range(8, 11)])
    selected = PaperAnalysisSource(
        tmp_path,
        source_factory=lambda: source,
        policy_factory=lambda _: POLICY,
        clock_ms=lambda: now[0],
    )
    feed = PaperFeedSupervisor(paper, selected, clock_ms=lambda: now[0])
    feed.run_once()
    assert paper.data["feed"]["warmed"] == {"s": True}, feed.status()
    paper.command("resume", {})
    return paper, source, selected, feed, now


def advance(source, now, index, *, missing=False, low="99"):
    candle = bar(index, low)
    source.rows.append(candle)
    if not missing:
        source.pins[candle.open_time_ms] = flow(candle)
    now[0] = candle.close_time_ms


def test_selected_inputs_warmup_and_atomic_live_receipt_do_not_reinject_late_revision(tmp_path):
    saved = []
    paper, source, selected, feed, now = setup(tmp_path, persist=saved.append)
    try:
        assert not paper.data["candles"] and not paper.detail("s")["pending"]
        feed.run_once()
        receipt = paper.detail("s")["analysis_input"]
        assert paper.detail("s")["pending"] and receipt["end_ms"] == 11 * MINUTE
        assert saved[-1]["strategies"]["s"]["analysis_input"] == receipt
        assert saved[-1]["strategies"]["s"]["runtime_checkpoint"]["last_candle"] == 10 * MINUTE
        validate_state(saved[-1])
        before = paper.features["s"].checkpoint()
        source.pins[10 * MINUTE] = flow(bar(10), "1000")
        feed.run_once()  # No new closed minute: no graph update or re-selection.
        assert paper.features["s"].checkpoint() == before
        assert paper.detail("s")["analysis_input"] == receipt
        assert source.calls == 2
    finally:
        feed.close()


def test_missing_aux_fills_old_order_and_allows_local_stop_while_manual_pause_stays(tmp_path):
    paper, source, selected, feed, now = setup(tmp_path)
    try:
        feed.run_once()
        paper.command("pause", {})
        advance(source, now, 11, missing=True, low="80")
        feed.run_once()
        row = paper.detail("s")
        assert row["quantity"] == "1" and len(row["fills"]) == 1
        assert row["pending"]["reducing"] and row["pending"]["reason"] == "local_test_stop"
        assert not row["analysis_input_health"]["ready"]
        assert not row["analysis_input_health"]["manual_recovery_required"]
        advance(source, now, 12)
        feed.run_once()
        row = paper.detail("s")
        assert row["quantity"] == "0" and len(row["fills"]) == 2
        assert row["analysis_input_health"]["ready"] and paper.data["paused"]
        assert row["pending"] is None
    finally:
        feed.close()


def test_aux_query_failure_is_durably_empty_and_auto_recovers_next_minute(tmp_path):
    paper, source, selected, feed, now = setup(tmp_path)
    try:
        source.fail = True
        feed.run_once()
        row = paper.detail("s")
        assert row["pending"] is None
        assert row["analysis_input_health"] == {
            "ready": False,
            "reason": "analysis_source_unavailable",
            "manual_recovery_required": False,
        }
        source.fail = False
        advance(source, now, 11)
        feed.run_once()
        assert paper.detail("s")["pending"] is not None
        assert paper.detail("s")["analysis_input_health"]["ready"]
        assert (
            selected.manifest.read(paper.detail("s")["analysis_input"]["head"])["previous"]
            is not None
        )
    finally:
        feed.close()


@pytest.mark.parametrize(
    "failure", ["wrong_candle", "wrong_binding", "bad_receipt_digest", "recording"]
)
def test_bad_selection_never_rolls_back_existing_fill_or_skips_protection(tmp_path, failure):
    paper, source, selected, feed, now = setup(tmp_path)
    try:
        feed.run_once()
        advance(source, now, 11, low="80")
        original = selected.prepare_inputs

        def broken(plan, candles, **kwargs):
            result = original(plan, candles, **kwargs)
            batch = result["s"]
            if failure == "recording":
                return {
                    "s": PaperInputBatch(batch.frames, None, "analysis_input_recording_unavailable")
                }
            if failure == "wrong_candle":
                return {
                    "s": replace(
                        batch,
                        frames=(MarketFeatureFrame(bar(10), kwargs["as_of_ms"]),),
                        receipt_json=None,
                        reason="bad_input",
                    )
                }
            receipt = batch.receipt
            receipt["binding" if failure == "wrong_binding" else "frame_digest"] = "f" * 64
            return {"s": replace(batch, receipt_json=json.dumps(receipt))}

        selected.prepare_inputs = broken
        feed.run_once()
        row = paper.detail("s")
        assert row["quantity"] == "1" and len(row["fills"]) == 1
        assert row["pending"]["reducing"]
        assert row["analysis_input_health"]["manual_recovery_required"]
        assert paper.features["s"].last_open_time_ms == 11 * MINUTE
        assert paper.data["strategies"]["s"]["runtime_checkpoint"]["last_candle"] == 11 * MINUTE
    finally:
        feed.close()


def test_persist_failure_keeps_cursor_feature_and_consumed_head_together(tmp_path):
    saved, failure = [], [False]

    def persist(state):
        if failure[0]:
            raise StorageError("injected uncertain paper save")
        saved.append(state)

    paper, source, selected, feed, now = setup(tmp_path, persist=persist)
    try:
        before, checkpoint = deepcopy(paper.data), paper.features["s"].checkpoint()
        failure[0] = True
        feed.run_once()
        assert paper.data["candles"] == before["candles"]
        assert paper.detail("s")["analysis_input"] == before["strategies"]["s"]["analysis_input"]
        assert paper.features["s"].checkpoint() == checkpoint
        assert paper.recording_health()["manual_resume_required"]
        assert selected.manifest.read(selected.verified["s"]["head"])["end_ms"] == 11 * MINUTE
        assert saved[-1]["strategies"]["s"]["analysis_input"]["end_ms"] == 10 * MINUTE
    finally:
        feed.close()


def test_cp_and_receipt_of_same_minute_different_input_cannot_be_mixed(tmp_path):
    paper, source, selected, feed, now = setup(tmp_path)
    try:
        feed.run_once()
        paper.command("paper.order.cancel", {"instance": "s"})
        saved = deepcopy(paper.data)
        row = saved["strategies"]["s"]
        frame = paper.features["s"].last_input_frame
        different = replace(
            frame,
            inputs=(
                replace(
                    frame.inputs[0],
                    values=(
                        ("AGGTRADE_NOTIONALBUY", Decimal(999)),
                        ("AGGTRADE_NOTIONALSELL", Decimal(2)),
                    ),
                ),
            ),
        )
        row["analysis_input"]["frame_digest"] = market_frame_checksum(different)
        restarted = PaperTradingService(factory, create_features, state=saved)
        with pytest.raises(ValidationError, match="checkpoint differs"):
            restarted.command("paper.strategy.recover", {"instance": "s"})
        assert restarted.data["paused"]
    finally:
        feed.close()


def test_paper_and_ca_manifest_have_distinct_writer_namespaces_and_kinds(tmp_path):
    paper, source, selected, feed, now = setup(tmp_path)
    ca = AnalysisInputManifest(tmp_path)
    try:
        feed.run_once()
        receipt = paper.detail("s")["analysis_input"]
        frames = replay(
            selected.manifest,
            receipt["head"],
            source,
            expected_analysis_id=receipt["binding"][:32],
            expected_policy=POLICY,
            expected_kind="paper_input",
        )
        head = ca.stage("b" * 32, "c" * 32, 0, frames, POLICY)
        assert ca.read(head)["kind"] == "ca_input"
        assert selected.manifest.read(receipt["head"])["kind"] == "paper_input"
        with pytest.raises(ValidationError, match="schema, identity"):
            replay(
                selected.manifest,
                receipt["head"],
                source,
                expected_analysis_id=receipt["binding"][:32],
                expected_policy=POLICY,
            )
    finally:
        ca.close()
        feed.close()


def test_slow_input_prepare_rechecks_actual_entry_clock_and_fixes_eligibility(tmp_path):
    paper, source, selected, feed, now = setup(tmp_path)
    try:
        original = selected.prepare_inputs

        def slow(*args, **kwargs):
            result = original(*args, **kwargs)
            now[0] += MINUTE
            return result

        selected.prepare_inputs = slow
        feed.run_once()
        assert paper.detail("s")["pending"] is None
        assert feed.status()["reason"] == "catching_up"
        assert paper.data["candles"][BTC.key][-1]["open_time_ms"] == 10 * MINUTE
    finally:
        feed.close()


def test_live_order_cannot_fill_bar_open_before_actual_decision_ms(tmp_path):
    paper, source, selected, feed, now = setup(tmp_path)
    try:
        now[0] += 5
        feed.run_once()
        assert paper.detail("s")["pending"]["eligible_at_ms"] == 11 * MINUTE + 5
        advance(source, now, 11)
        feed.run_once()
        assert paper.detail("s")["quantity"] == "0"
        advance(source, now, 12)
        feed.run_once()
        assert paper.detail("s")["quantity"] == "1"
        assert paper.detail("s")["fills"][0]["timestamp_ms"] == 12 * MINUTE
    finally:
        feed.close()


def test_worker_closes_manifest_on_own_thread_and_reports_cleanup_failure(tmp_path):
    paper, source, selected, feed, now = setup(tmp_path)
    feed.close()
    # Build a fresh owner/source; a manifest already used by the synchronous test
    # thread is intentionally not transferable to a background thread.
    restored = PaperTradingService(factory, create_features, state=paper.data)
    restored.command("paper.strategy.recover", {"instance": "s"})
    source = Source([bar(i) for i in range(8, 11)])
    owner_threads, closed = [], threading.Event()

    def manifest_factory():
        owner_threads.append(threading.get_ident())
        writer = AnalysisInputManifest(
            tmp_path, namespace="paper-inputs", consumer_kind="paper_input"
        )
        close = writer.close

        def failing_close():
            assert threading.get_ident() == owner_threads[0]
            close()
            closed.set()
            raise StorageError("injected close failure")

        writer.close = failing_close
        return writer

    selected = PaperAnalysisSource(
        tmp_path,
        source_factory=lambda: source,
        manifest_factory=manifest_factory,
        policy_factory=lambda _: POLICY,
        clock_ms=lambda: now[0],
    )
    feed = PaperFeedSupervisor(restored, selected, clock_ms=lambda: now[0], poll_seconds=0.05)
    feed.start()
    for _ in range(100):
        if selected.manifest is not None:
            break
        closed.wait(0.01)
    with pytest.raises(RuntimeError, match="cleanup failed"):
        feed.close()
    assert closed.is_set() and source.closed and len(owner_threads) == 1
    assert feed.status()["reason"] == "source_close_failed"
    second = AnalysisInputManifest(tmp_path, namespace="paper-inputs", consumer_kind="paper_input")
    try:
        second._writer()  # The failed close still released the exclusive owner.
    finally:
        second.close()


def test_staged_checkpoint_has_separate_input_head_and_recovers_before_promotion(tmp_path):
    paper, source, selected, feed, now = setup(tmp_path)
    try:
        paper.command("pause", {})
        now[0] = 3001 * MINUTE
        paper.feed_configure(True, start_ms=3000 * MINUTE, warmup_minutes=2001)
        plan = paper.feed_snapshot()
        item = plan["warmup"]["s"]
        # One bounded chunk is committed; the active graph/head stays at the
        # previously completed short warmup until final staged promotion.
        low = item["next_open_ms"]
        source.rows = [bar(i) for i in range(low // MINUTE, low // MINUTE + 512)]
        source.pins = {c.open_time_ms: flow(c) for c in source.rows}
        rows = tuple(source.rows)
        inputs = selected.prepare_inputs(
            plan, rows, as_of_ms=now[0], names={"s"}, retrospective=True
        )
        prior = deepcopy(paper.data["strategies"]["s"]["analysis_input"])
        paper.feed_warmup_chunk("s", rows, expected=plan, now_ms=now[0], analysis_inputs=inputs)
        stage = paper.data["feed"]["warmup_plans"]["s"]
        assert stage["analysis_input"]["end_ms"] == rows[-1].close_time_ms
        assert paper.data["strategies"]["s"]["analysis_input"] == prior
        validate_state(paper.data)
        restarted = PaperTradingService(factory, create_features, state=paper.data)
        result = restarted.command("paper.strategy.recover", {"instance": "s"})
        assert result["restored_checkpoint"]
        assert (
            restarted.feed_snapshot()["analysis_inputs"]["s"]["receipt"] == stage["analysis_input"]
        )
        mixed = deepcopy(stage)
        mixed["analysis_input"]["frame_digest"] = "f" * 64
        with pytest.raises(ValidationError, match="checkpoint differs"):
            restarted._staged_feature("s", mixed)
    finally:
        feed.close()


def add_cold(paper, instrument):
    paper.command("paper.account.create", {"account_id": "b", "balance": "10000"})
    paper.command(
        "paper.strategy.add",
        {
            "account_id": "b",
            "instance": "cold",
            "instrument": instrument.key,
            "module_id": "nes_target",
            "configuration": {"quantity": "1"},
            "budget": "1000",
            "analysis": {"order_flow": [{"interval_id": 0, "analysis_code": "NES", "nSamples": 2}]},
        },
    )


def test_cold_new_instrument_does_not_starve_old_pending_and_protection_then_joins_frontier(
    tmp_path,
):
    paper, source, selected, feed, now = setup(tmp_path)
    eth = Instrument("binance", Market.USDM, "ETHUSDT")
    try:
        feed.run_once()
        add_cold(paper, eth)
        paper.command("pause", {})
        advance(source, now, 11, low="80")
        feed.run_once()  # ETH warmup lacks both candles and auxiliary facts.
        assert paper.detail("s")["quantity"] == "1"
        assert paper.detail("s")["pending"]["reducing"]
        assert paper.detail("cold")["last_candle"] is None
        assert set(paper.feed_snapshot()["instruments"]) == {BTC.key}
        advance(source, now, 12)
        feed.run_once()
        assert paper.detail("s")["quantity"] == "0"
        assert len(paper.detail("s")["fills"]) == 2
        assert eth.key not in paper.data["candles"]
        for index in (11, 12):
            candle = replace(bar(index), instrument=eth)
            source.rows.append(candle)
            source.secondary[candle.open_time_ms] = flow(candle)
        feed.run_once()  # The cold graph primes through the current live frontier.
        assert not paper.feed_snapshot()["warmup"], feed.status()
        assert (
            paper.feed_snapshot()["instruments"][eth.key]["last_candle"]["open_time_ms"]
            == 12 * MINUTE
        )
        assert {v["next_open_ms"] for v in paper.feed_snapshot()["instruments"].values()} == {
            13 * MINUTE
        }
        advance(source, now, 13)
        candle = replace(bar(13), instrument=eth)
        source.rows.append(candle)
        source.secondary[candle.open_time_ms] = flow(candle)
        anchor = next(
            c for c in source.rows if c.instrument == eth and c.open_time_ms == 12 * MINUTE
        )
        source.rows.remove(anchor)
        feed.run_once()
        assert feed.status()["reason"] == "committed_cursor_conflict_or_missing"
        assert paper.detail("cold")["last_candle"] is None
        source.rows.append(anchor)
        feed.run_once()
        assert paper.detail("cold")["last_candle"] == 13 * MINUTE
        assert paper.detail("cold")["analysis_input_health"]["ready"]
        assert paper.data["paused"] and paper.detail("cold")["pending"] is None
        validate_state(paper.data)
    finally:
        feed.close()


def test_cold_same_instrument_aux_warmup_failure_does_not_skip_live_model_clock(tmp_path):
    paper, source, selected, feed, now = setup(tmp_path)
    try:
        feed.run_once()
        add_cold(paper, BTC)
        advance(source, now, 11, low="80")
        source.fail = True
        feed.run_once()
        assert paper.detail("s")["quantity"] == "1"
        assert paper.detail("s")["pending"]["reducing"]
        assert paper.features["s"].last_open_time_ms == 11 * MINUTE
        assert paper.detail("cold")["last_candle"] is None
        assert paper.detail("cold")["pending"] is None
        assert paper.data["feed"]["reason"] == "warmup"
        assert not paper.data["feed"]["ready"]
        assert "cold" in paper.feed_snapshot()["warmup"]
    finally:
        feed.close()
