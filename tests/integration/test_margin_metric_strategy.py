"""Native metric policy, pinned replay and real shared virtual finance; no DB/network.

The declared test TS consumes a real MST result. Built-in PATHFINDER separately
proves its complete graph/native input contract during honest numeric warmup.
"""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_margin_native_depth_strategy import BTC, PinStore, bar, config, prepared
from test_margin_native_depth_strategy import Registry as DepthRegistry

from omega.analysis.catalog import create_features
from omega.analysis.graph import FeatureNode
from omega.analysis.market_graph import MarketFeatureGraph
from omega.analysis.market_sentiment import LegacyMarketSentimentFeatures, SentimentConfiguration
from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.application.market_frame_builder import MarketFrameBuilder
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market, Side
from omega.domain.trading import OrderProposal, TradeDecision
from omega.ports.auxiliary import StoredAuxiliary
from omega.ports.modules import ModuleDescriptor
from omega.runtime import margin_session
from omega.runtime.margin_metric_inputs import MetricInputPolicy
from omega.runtime.margin_session import MarginTradingSession
from omega.runtime.margin_session_inputs import MarginSessionInputs
from omega.runtime.margin_trading import MarginTradingService

DESTINATION = "d" * 64
FIELD = "0_MST_0_MST"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


class MetricConsumer:
    descriptor = ModuleDescriptor("mst_fixture", "1", frozenset({FIELD}), True)

    def __init__(self):
        self.last_open_time_ms = None

    def checkpoint(self):
        return {"cursor": self.last_open_time_ms}

    def restore(self, value):
        self.last_open_time_ms = value["cursor"]

    def resume(self):
        pass

    def evaluate_protection(self, context):
        self.last_open_time_ms = context.closed_candle.open_time_ms
        if context.position.quantity and context.closed_candle.low <= D(95):
            return TradeDecision(
                (OrderProposal(Side.SELL, abs(context.position.quantity), reducing=True),),
                "metric_fixture_stop",
            )
        return TradeDecision(reason="metric_fixture_hold")

    def evaluate(self, context):
        self.last_open_time_ms = context.closed_candle.open_time_ms
        if (
            context.features[FIELD] is not None
            and not context.position.quantity
            and context.entries_allowed
        ):
            return TradeDecision(
                (OrderProposal(Side.BUY, D(1)),), "metric_fixture_calculated_entry"
            )
        return self.evaluate_protection(context)


class Registry:
    def create(self, module, _instrument, _configuration):
        assert module == "mst_fixture"
        return MetricConsumer()


class Samples(PinStore):
    def latest_sample(self, instrument, channel, source, *, event_before_ms, as_of_ms, max_age_ms):
        rows = [
            row
            for row in self.records
            if row.instrument == instrument
            and row.observation.channel == channel
            and row.observation.source == source
            and max(0, event_before_ms - max_age_ms)
            <= row.observation.event_time_ms
            < event_before_ms
            and row.observation.available_at_ms <= as_of_ms
        ]
        return max(
            rows, key=lambda row: (row.observation.event_time_ms, row.revision), default=None
        )


def native(channel, *, event=0, available=None, number=D(1000), revision=1, previous=None):
    suffix, field = (
        ("open_interest_history", "METRIC_OPENINTEREST")
        if channel == "open_interest"
        else ("long_short_ratio", "METRIC_LONGSHORTRATIO")
    )
    opening = event // 60000 * 60000
    value = AuxiliaryInput(
        channel,
        ((field, number), ("SOURCE_PERIOD_MS", 300000)),
        opening,
        opening + 60000,
        event,
        event if available is None else available,
        "observed",
        D(1),
        BTC.key + ":" + suffix,
    )
    return StoredAuxiliary(
        BTC, value, revision, observation_checksum(encode_observation(BTC, value)), previous
    )


def frame(
    index, *, missing=False, zero=False, low="99", age=330000, as_of=None, revision=1, previous=None
):
    ordinary = prepared(index, low=low)
    candle = ordinary.frame.candle
    cutoff = candle.close_time_ms if as_of is None else as_of
    records = ordinary.pins
    if not missing:
        event = index // 5 * 300000  # Explicit synthetic five-minute native sample schedule.
        original = native("open_interest", event=event)
        # A zero correction is a new received revision of that exact native key;
        # it cannot overwrite the earlier positive revision or its availability.
        selected = native(
            "open_interest",
            event=event,
            number=D(0) if zero else D(1000),
            revision=2 if zero else revision,
            previous=original.checksum if zero else previous,
            available=cutoff if zero or revision > 1 else event,
        )
        records += (
            selected,
            native("long_short_ratio", event=event, number=D("1.5")),
        )
    return MarketFrameBuilder(Samples(records), maximum_metric_age_ms=age).prepare(
        candle,
        as_of_ms=cutoff,
        channels=("metric", "trade_flow"),
    )


def configuration(*, pathfinder=False, age=330000, extra_line=None):
    account, strategies, brackets, _ = config()
    if pathfinder:
        fixture = json.loads(
            (FIXTURES / "tef-pathfinder-native-inputs.json").read_text(encoding="utf-8")
        )
        strategies[0].update(
            module="tef",
            configuration=fixture["strategy_configuration"],
            analysis=fixture["analysis"],
        )
    else:
        lines = [{"interval_id": 0, "nSamples": 2, "line_index": 0, "maximum_metric_age_ms": age}]
        if extra_line is not None:
            lines.append(
                {
                    "interval_id": 0,
                    "nSamples": 2,
                    "line_index": 1,
                    "maximum_metric_age_ms": extra_line,
                }
            )
        strategies[0].update(
            module="mst_fixture", configuration={}, analysis={"market_sentiment": lines}
        )
    return account, strategies, brackets


def session(root, *, create=True, **options):
    account, strategies, _ = configuration(**options)
    return MarginTradingSession(
        root,
        account,
        strategies,
        0,
        create=create,
        **({} if options.get("pathfinder") else {"registry": Registry()}),
    )


def step(owner, value, *, destination=DESTINATION):
    return owner.run_once(
        (value.frame.candle,),
        as_of_ms=value.frame.as_of_ms,
        brackets=configuration()[2],
        prepared_frames=(value,),
        auxiliary_destination=destination,
    )


def test_public_metric_horizon_uses_actual_all_line_configs_and_preserves_checkpoint():
    configurations = (
        SentimentConfiguration(0, nSamples=2, maximum_metric_age_ms=0),
        SentimentConfiguration(0, nSamples=2, line_index=1, maximum_metric_age_ms=330000),
    )
    provider = LegacyMarketSentimentFeatures(BTC, configurations)
    before = provider.checkpoint()
    assert provider.maximum_metric_age_ms == 330000 and provider.checkpoint() == before
    graph = create_features(BTC, {FIELD}, configuration(extra_line=120000)[1][0]["analysis"])
    frozen = graph.checkpoint()
    assert graph.maximum_metric_age_ms == 330000 and graph.checkpoint() == frozen
    zero = create_features(BTC, {FIELD}, configuration(age=0)[1][0]["analysis"])
    assert zero.maximum_metric_age_ms == 0
    ordinary = create_features(
        BTC, {"0_NES_0_NES"}, {"order_flow": [{"interval_id": 0, "analysis_code": "NES"}]}
    )
    assert ordinary.maximum_metric_age_ms is None


@pytest.mark.parametrize("age", [None, True, 1.0, -1, 86400001])
def test_declared_metric_provider_needs_public_native_policy_before_finance(age):
    feature = SimpleNamespace(
        descriptor=ModuleDescriptor("invalid_metric_fixture", "1", frozenset({"metric"}), True),
        update=lambda _bar: {},
        maximum_metric_age_ms=age,
    )
    graph = MarketFeatureGraph(BTC, (FeatureNode("invalid", feature),))
    with pytest.raises(ValidationError, match="policy"):
        _ = graph.maximum_metric_age_ms
    with pytest.raises(ValidationError, match="policy"):
        MarginSessionInputs([{"id": "metric", "instrument": BTC}], {"metric": graph})


def test_metric_schema6_is_explicit_and_old_depth_flow_schema5_hash_is_exact(tmp_path):
    metric = session(tmp_path / "metric")
    try:
        assert metric._meta["schema_version"] == 6
        expected = {BTC.key: MetricInputPolicy(330000).document()}
        assert metric.analysis_metric_policies == expected
        metric.analysis_metric_policies[BTC.key]["maximum_metric_age_ms"] = 0
        assert metric._meta["configuration"]["market_inputs"]["metric_policies"] == expected
    finally:
        metric.close()
    account, strategies, _brackets, _ = config()
    old = MarginTradingSession(
        tmp_path / "old", account, strategies, 0, registry=DepthRegistry(), create=True
    )
    try:
        expected = {
            "account": account,
            "strategies": strategies,
            "start_ms": 0,
            "fee_rate": "0.001",
            "model": "ohlcv_margin_session_v1",
            "market_inputs": {"channels": {BTC.key: ["depth", "trade_flow"]}},
        }
        assert old._meta["schema_version"] == 5 and old._meta["configuration"] == expected
        assert (
            old.config_hash
            == sha256(
                json.dumps(
                    expected, sort_keys=True, separators=(",", ":"), allow_nan=False
                ).encode()
            ).hexdigest()
        )
        assert old.analysis_metric_policies == {}
    finally:
        old.close()


def test_real_mst_calculation_then_missing_metric_keeps_fill_protection_and_healthy_recovery(
    tmp_path,
):
    owner = session(tmp_path)
    try:
        owner.resume("explicit_metric_fixture_resume")
        for index in range(6):
            value = frame(index, missing=index in {3, 4}, low="90" if index == 3 else "99")
            view = step(owner, value)
            assert view["graph_errors"] == {}
            ledger = owner.execution.snapshot().account.ledger
            if index == 2:
                assert owner._meta["last_frame"]["features"]["flow"][FIELD] is not None
                assert view["pending_orders"] and not ledger
            elif index == 3:
                assert len(ledger) == 1 and ledger[0].fact.delta_qty == 1
                assert view["market_inputs"]["state"] == "unavailable" and view["pending_orders"]
                assert owner._meta["last_frame"]["features"]["flow"][FIELD] is None
            elif index == 4:
                assert [item.fact.delta_qty for item in ledger] == [D(1), D(-1)]
                assert owner.execution.snapshot().account.positions[0].quantity == 0
            elif index == 5:
                assert view["market_inputs"]["state"] == "ready" and view["pending_orders"]
        assert [item.fact.event_time_ms for item in ledger] == [180000, 240000]
    finally:
        owner.close()


def test_sparse_carried_metric_replays_native_pins_but_zero_never_exposes_held_mst(tmp_path):
    owner = session(tmp_path)
    try:
        for index in range(4):
            value = frame(index, zero=index == 3)
            view = step(owner, value)
            assert view["graph_errors"] == {}
            if index == 2:
                assert value.frame.input("metric").quality == "carried"
                assert view["market_inputs"]["state"] == "ready"
                assert owner._meta["last_frame"]["features"]["flow"][FIELD] is not None
            if index == 3:
                assert value.frame.input("metric").mapping()["METRIC_OPENINTEREST"] == 0
                correction = next(
                    pin for pin in value.pins if pin.observation.channel == "open_interest"
                )
                assert (
                    correction.revision == 2
                    and correction.previous_checksum == native("open_interest").checksum
                )
                assert correction.observation.available_at_ms == 240000
                assert owner._meta["last_frame"]["graph_ready"]["flow"] is False
                assert owner._meta["last_frame"]["features"]["flow"][FIELD] is None
                assert view["market_inputs"]["state"] == "unavailable"
    finally:
        owner.close()


@pytest.mark.parametrize(
    "fault",
    [
        "derived_value",
        "source_time",
        "source",
        "checksum",
        "revision",
        "foreign",
        "orphan",
        "future_cutoff",
    ],
)
def test_changed_metric_or_native_pin_rejects_before_session_or_financial_mutation(tmp_path, fault):
    owner = session(tmp_path)
    try:
        value = frame(0)
        metric = value.frame.input("metric")
        if fault in {"derived_value", "source_time", "source"}:
            if fault == "source":
                changed = replace(metric, source="metric_view:" + "0" * 64)
            else:
                values = metric.mapping()
                name = (
                    "METRIC_OPENINTEREST"
                    if fault == "derived_value"
                    else "SOURCE_OPENINTEREST_TIME_MS"
                )
                values[name] += D(1) if fault == "derived_value" else 1
                changed = replace(metric, values=tuple(values.items()))
            value = replace(
                value,
                frame=replace(
                    value.frame,
                    inputs=tuple(
                        changed if row.channel == "metric" else row for row in value.frame.inputs
                    ),
                ),
            )
        elif fault == "future_cutoff":
            value = replace(value, frame=replace(value.frame, as_of_ms=60001))
        else:
            pins = list(value.pins)
            if fault == "checksum":
                pins[0] = replace(pins[0], checksum="0" * 64)
            elif fault == "revision":
                object.__setattr__(pins[0], "revision", True)
            elif fault == "foreign":
                pins[0] = replace(pins[0], instrument=Instrument("binance", Market.USDM, "ETHUSDT"))
            else:
                pins.append(pins[0])
            value = replace(value, pins=tuple(pins))
        before = owner.path.read_bytes()
        with pytest.raises((ProtocolError, ValidationError)):
            owner.run_once(
                (bar(0),),
                as_of_ms=60000,
                brackets=configuration()[2],
                prepared_frames=(value,),
                auxiliary_destination=DESTINATION,
            )
        assert owner.path.read_bytes() == before and owner.snapshot()["cursor_ms"] is None
        assert not owner.execution.snapshot().account.ledger
    finally:
        owner.close()


def test_metric_boundary_latency_and_one_missing_source_keep_exact_eligible_native_pins(tmp_path):
    owner = session(tmp_path / "latency", age=60000)
    try:
        value = frame(0, age=60000, as_of=60001)
        assert value.frame.input("metric").quality == "missing"
        assert {pin.observation.channel for pin in value.pins} == {
            "open_interest",
            "long_short_ratio",
            "trade_flow",
        }
        view = step(owner, value)
        assert view["graph_errors"] == {} and view["market_inputs"]["state"] == "unavailable"
        saved = owner._meta["last_frame"]["market_inputs"]
        assert len(saved["frames"][0]["pins"]) == 3
        assert all(
            pin.observation.event_time_ms == 0
            for pin in value.pins
            if pin.observation.channel != "trade_flow"
        )
    finally:
        owner.close()
    owner = session(tmp_path / "sparse")
    try:
        ordinary = prepared(0)
        source = Samples(ordinary.pins + (native("open_interest"),))
        value = MarketFrameBuilder(source, maximum_metric_age_ms=330000).prepare(
            bar(0), as_of_ms=60000, channels=("metric", "trade_flow")
        )
        view = step(owner, value)
        assert view["market_inputs"]["state"] == "unavailable" and len(value.pins) == 2
        assert value.pins[0].observation.channel == "open_interest"
        assert len(owner._meta["last_frame"]["market_inputs"]["frames"][0]["pins"]) == 2
    finally:
        owner.close()


def test_tight_mst_line_remains_unready_despite_shared_looser_selection_horizon(tmp_path):
    owner = session(tmp_path, age=60000, extra_line=330000)
    try:
        assert owner.analysis_metric_policies[BTC.key]["maximum_metric_age_ms"] == 330000
        first = step(owner, frame(0))
        assert (
            first["graph_errors"] == {} and owner._meta["last_frame"]["graph_ready"]["flow"] is True
        )
        second = step(owner, frame(1))
        assert (
            second["graph_errors"] == {}
            and owner._meta["last_frame"]["graph_ready"]["flow"] is False
        )
        assert owner._meta["last_frame"]["features"]["flow"][FIELD] is None
    finally:
        owner.close()


def test_frozen_metric_ack_loss_restart_and_policy_change_never_select_new_sources(
    tmp_path, monkeypatch
):
    owner = session(tmp_path)
    original_write = margin_session.atomic_json

    def lost_ack(path, envelope):
        original_write(path, envelope)
        if envelope["data"].get("pending") is not None:
            raise OSError("metric candidate ACK lost")

    try:
        value = frame(0)
        monkeypatch.setattr(margin_session, "atomic_json", lost_ack)
        with pytest.raises(StorageError):
            step(owner, value)
        monkeypatch.setattr(margin_session, "atomic_json", original_write)
        assert owner._read() == owner._uncertain[1]
        owner.reconcile()
        frozen = deepcopy(owner._meta["pending"]["market_inputs"])
        newer = frame(0, revision=2, previous=value.pins[0].checksum)
        assert newer.pins[0].observation.available_at_ms == 60000
        with pytest.raises(ProtocolError, match="replaced"):
            step(owner, newer)
    finally:
        monkeypatch.setattr(margin_session, "atomic_json", original_write)
        owner.close()
    restored = session(tmp_path, create=False)
    try:
        assert restored._meta["pending"]["market_inputs"] == frozen
        restored.run_once((), as_of_ms=60001)
        assert restored._meta["last_frame"]["market_inputs"] == frozen
        assert restored._graphs["flow"].last_input_frame == value.frame
        path, before = restored.path, restored.path.read_bytes()
    finally:
        restored.close()
    with pytest.raises(ProtocolError, match="configuration"):
        session(tmp_path, create=False, age=60000)
    assert path.read_bytes() == before


def test_complete_builtin_pathfinder_graph_accepts_native_union_during_honest_warmup(tmp_path):
    owner = session(tmp_path, pathfinder=True)
    try:
        fixture = json.loads((FIXTURES / "tef-pathfinder-native-inputs.json").read_text())
        assert owner._modules["flow"].descriptor.required_fields == frozenset(
            fixture["required_fields"]
        )
        assert owner.analysis_input_channels == {BTC.key: ("depth", "metric", "trade_flow")}
        ordinary = prepared(0)
        records = ordinary.pins + (
            native("open_interest"),
            native("long_short_ratio", number=D("1.5")),
        )
        value = MarketFrameBuilder(Samples(records), maximum_metric_age_ms=330000).prepare(
            bar(0), as_of_ms=60000, channels=("depth", "metric", "trade_flow")
        )
        view = step(owner, value)
        assert view["graph_errors"] == {} and view["state"] == "ready"
        assert (
            set(fixture["required_fields"]) <= owner._meta["last_frame"]["features"]["flow"].keys()
        )
        assert owner._meta["last_frame"]["features"]["flow"]["5_MST_0_MST"] is None
        assert not owner._modules["flow"].history_priming_ready
        assert owner._modules["flow"].snapshot()["model_failure"] is None
        assert not view["pending_orders"] and not owner.execution.snapshot().account.ledger
    finally:
        owner.close()


def test_runtime_forwards_frozen_metric_horizon_and_optional_failure_returns_missing():
    value = frame(0)
    calls = []

    def read_frames(*args, **kwargs):
        calls.append(kwargs)
        return (value,)

    runtime = object.__new__(MarginTradingService)
    runtime.source = SimpleNamespace(destination_id=DESTINATION, read_frames=read_frames)
    selected = SimpleNamespace(
        analysis_input_channels={BTC.key: ("metric", "trade_flow")},
        analysis_metric_policies={BTC.key: MetricInputPolicy(330000).document()},
    )
    options, reason = runtime._analysis_inputs(selected, {}, (bar(0),), as_of_ms=60000)
    assert reason is None and options["prepared_frames"] == (value,)
    assert calls[0]["maximum_metric_age_ms"] == 330000

    def failure(*_args, **_kwargs):
        raise TimeoutError("optional fixture read")

    runtime.source.read_frames = failure
    options, reason = runtime._analysis_inputs(selected, {}, (bar(0),), as_of_ms=60000)
    assert reason == "auxiliary_read_TimeoutError"
    missing = options["prepared_frames"][0]
    assert not missing.pins and missing.frame.input("metric").quality == "missing"
    assert missing.frame.input("trade_flow").quality == "missing"
