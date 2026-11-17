"""Original synthetic source clocks, real V2 bundle/index and shared financial owner."""

import json
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256
from pathlib import Path

import pytest
from test_margin_cost_research import policy

from omega.analysis.order_flow import DEPTH_FIELDS
from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.application.native_metric_inputs import MetricInputPolicy
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market, Side
from omega.domain.trading import OrderProposal, TradeDecision
from omega.infrastructure.binance.depth_book import BookView
from omega.infrastructure.binance.depth_sampling import DepthMinuteSampler
from omega.ports.auxiliary import AuxiliaryReadRange, StoredAuxiliary
from omega.ports.modules import ModuleDescriptor
from omega.research.artifacts import Cancelled
from omega.research.auxiliary_datasets import export_auxiliary_dataset, load_auxiliary_dataset
from omega.research.datasets import MarketRow, export_dataset, write_rows
from omega.research.margin_observation_inputs import NativeObservationReplay
from omega.research.margin_simulation import simulate_margin
from omega.runtime.margin_console import scenario_brackets
from omega.runtime.margin_research import run_margin_simulation_job
from omega.runtime.margin_session import MarginTradingSession, prepare_margin_configuration
from omega.strategies.registry import StrategyRegistry

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
MINUTE = 60000
FIELDS = ("0_MST_0_MST", "0_NES_0_NES", "0_WOI_0_WOI")
SUFFIXES = {
    "trade_flow": "aggTrade",
    "depth": "binance_depth_ws_v1",
    "open_interest": "open_interest_history",
    "long_short_ratio": "long_short_ratio",
}


def pin(channel, opening, values, *, event=None, available=None, quality="observed", coverage=D(1)):
    event = opening + MINUTE if event is None else event
    observation = AuxiliaryInput(
        channel,
        tuple(values),
        opening,
        opening + MINUTE,
        event,
        event if available is None else available,
        quality,
        coverage,
        BTC.key + ":" + SUFFIXES[channel],
    )
    return StoredAuxiliary(
        BTC, observation, 1, observation_checksum(encode_observation(BTC, observation)), None
    )


def source(
    root,
    *,
    count=5,
    start=MINUTE,
    lookback=330000,
    delay=0,
    missing_after=None,
    correction=False,
    partial_depth=None,
    channels=None,
    varied=False,
):
    root.mkdir()
    channels = tuple(SUFFIXES) if channels is None else channels
    rows = []
    for index in range(count):
        phase = index % 120
        wave = phase if phase < 60 else 120 - phase
        close = D(100) + (D(index) / 100 + D(wave) / 10 if varied else D(0))
        opening = rows[-1].close if rows else close
        rows.append(
            MarketRow(
                BTC.key,
                start + index * MINUTE,
                opening,
                max(opening, close) + 2,
                D(90)
                if missing_after is not None and index >= missing_after
                else min(opening, close) - 2,
                close,
                D(10),
            )
        )
    write_rows(root / "source.csv", rows)
    export_dataset(root / "source.csv", root / "candles", {})
    end, records = rows[-1].open_time_ms + MINUTE, []
    sampler = DepthMinuteSampler(BTC, price_precision=8, quote_precision=8)
    sampler.begin(1, started_at_ms=start)
    for index, row in enumerate(rows):
        if missing_after is not None and index >= missing_after:
            continue
        close = row.open_time_ms + MINUTE
        if "trade_flow" in channels:
            trade_price = (row.open + row.close) / 2
            records.append(
                pin(
                    "trade_flow",
                    row.open_time_ms,
                    (
                        ("AGGTRADE_QUANTITYBUY", D(8)),
                        ("AGGTRADE_QUANTITYSELL", D(2)),
                        ("AGGTRADE_NOTIONALBUY", trade_price * 8),
                        ("AGGTRADE_NOTIONALSELL", trade_price * 2),
                        ("AGGTRADE_NTRADESBUY", 4),
                        ("AGGTRADE_NTRADESSELL", 1),
                    ),
                    available=close + delay,
                )
            )
        if "depth" in channels:
            radii = (
                (D(".0005"),)
                if partial_depth == index
                else tuple(map(D, (".0005", ".005", ".015", ".025", ".035", ".045", ".055")))
            )
            bids = tuple((row.close * (1 - radius), D(8 + index % 17)) for radius in radii)
            asks = tuple((row.close * (1 + radius), D(2 + index % 13)) for radius in radii)
            assert sampler.observe(
                BookView(index, bids, asks, bids[-1][0], asks[-1][0]),
                event_time_ms=close - 1,
                received_at_ms=close - 1 + delay,
                accepted_at_ms=close - 1 + delay,
                generation=1,
                raw_refs=("a" * 64,),
            )
            observation = sampler.finalize(row.open_time_ms, as_of_ms=close + delay).observation
            assert observation.quality == ("partial" if partial_depth == index else "observed")
            records.append(
                StoredAuxiliary(
                    BTC,
                    observation,
                    1,
                    observation_checksum(encode_observation(BTC, observation)),
                    None,
                )
            )
    native_start = max(0, start - lookback) // MINUTE * MINUTE
    for event in range(native_start // 300000 * 300000, end, 300000):
        if event < native_start:
            continue
        for channel, field, value in (
            ("open_interest", "METRIC_OPENINTEREST", D(1000 + event // MINUTE)),
            ("long_short_ratio", "METRIC_LONGSHORTRATIO", D("1.1") + D(event // 300000 % 5) / 100),
        ):
            if channel not in channels:
                continue
            original = pin(
                channel, event, ((field, value), ("SOURCE_PERIOD_MS", 300000)), event=event
            )
            records.append(original)
            if correction and channel == "open_interest" and event == 0:
                observation = replace(
                    original.observation,
                    values=((field, D(0)), ("SOURCE_PERIOD_MS", 300000)),
                    available_at_ms=start + MINUTE + 1,
                )
                records.append(
                    StoredAuxiliary(
                        BTC,
                        observation,
                        2,
                        observation_checksum(encode_observation(BTC, observation)),
                        original.checksum,
                    )
                )
    ranges = tuple(
        AuxiliaryReadRange(
            BTC,
            channel,
            BTC.key + ":" + SUFFIXES[channel],
            native_start if channel in {"open_interest", "long_short_ratio"} else start,
            end,
        )
        for channel in channels
    )
    bundle = root / "bundle"
    export_auxiliary_dataset(
        root / "candles",
        bundle,
        records,
        format_version=2,
        source_ranges=ranges,
        metric_lookback_ms=lookback
        if set(channels) & {"open_interest", "long_short_ratio"}
        else None,
        as_of_ms=end + 120000,
    )
    manifest, restored_rows, selected = load_auxiliary_dataset(bundle)
    return bundle, manifest, restored_rows, selected


def bar(row):
    return Candle(
        BTC,
        row.open_time_ms,
        row.open_time_ms + MINUTE,
        row.open,
        row.high,
        row.low,
        row.close,
        row.volume,
    )


def bind(replay, rows, channels=("depth", "metric", "trade_flow"), age=330000):
    requirements = {BTC.key: channels}
    policies = {BTC.key: MetricInputPolicy(age).document()} if "metric" in channels else {}
    replay.bind_configuration(
        requirements, policies, start_ms=rows[0].open_time_ms, end_ms=rows[-1].open_time_ms + MINUTE
    )
    return requirements, policies


def prepare(replay, row, requirements):
    candles = (bar(row),)
    return replay.prepare(candles, as_of_ms=replay.as_of_ms(candles), requirements=requirements)[0]


def test_native_metric_application_aliases_preserve_original_public_class_identity():
    from omega.application.native_metric_inputs import rebuild_metric, validate_metric
    from omega.runtime import margin_metric_inputs as legacy

    assert legacy.MetricInputPolicy is MetricInputPolicy
    assert legacy.rebuild_metric is rebuild_metric
    assert legacy.validate_metric is validate_metric


def test_seed_retained_revisions_cutoff_zero_and_depth_quality_stay_original(tmp_path):
    _bundle, manifest, rows, records = source(tmp_path / "source", correction=True, partial_depth=1)
    replay = NativeObservationReplay(manifest, records)
    with pytest.raises(ValidationError, match="bound"):
        _ = replay.lineage
    requirements, policies = bind(replay, rows)
    destination = replay.destination_id
    first = prepare(replay, rows[0], requirements)
    first_oi = next(p for p in first.pins if p.observation.channel == "open_interest")
    assert (
        first_oi.revision == 1 and first_oi.observation.window_start_ms == 0 < rows[0].open_time_ms
    )
    second = prepare(replay, rows[1], requirements)
    second_oi = next(p for p in second.pins if p.observation.channel == "open_interest")
    assert second_oi.revision == 2 and second_oi.previous_checksum == first_oi.checksum
    assert second_oi.observation.mapping()["METRIC_OPENINTEREST"] == 0
    depth = next(item for item in second.frame.inputs if item.channel == "depth")
    assert depth.quality == "partial" and depth.coverage == 0
    assert not any(name in DEPTH_FIELDS for name, _ in depth.values)
    bind(replay, rows)
    replay.validate_configuration(requirements, policies)
    assert replay.destination_id == destination
    object.__setattr__(second_oi, "checksum", "e" * 64)
    assert prepare(replay, rows[1], requirements).pins != second.pins
    assert (
        replay.lineage["selection"]["revision_coverage"]
        == "all_retained_revisions_known_by_export_cutoff"
    )


@pytest.mark.parametrize("damage", ["horizon", "source", "foreign", "policy_bool"])
def test_invalid_native_graph_plan_rejects_before_lineage_binding(tmp_path, damage):
    _bundle, manifest, rows, records = source(
        tmp_path / "source", channels=("trade_flow", "open_interest", "long_short_ratio")
    )
    replay = NativeObservationReplay(manifest, records)
    requirements = {BTC.key: ("metric", "trade_flow")}
    policies = {BTC.key: MetricInputPolicy(330000).document()}
    if damage == "horizon":
        policies[BTC.key]["maximum_metric_age_ms"] = 330001
    elif damage == "source":
        requirements[BTC.key] = ("depth", "metric", "trade_flow")
    elif damage == "foreign":
        requirements["binance:usdm:ETHUSDT"] = ("trade_flow",)
    else:
        policies[BTC.key]["maximum_metric_age_ms"] = True
    with pytest.raises(ValidationError):
        replay.bind_configuration(
            requirements,
            policies,
            start_ms=rows[0].open_time_ms,
            end_ms=rows[-1].open_time_ms + MINUTE,
        )
    with pytest.raises(ValidationError, match="bound"):
        _ = replay.destination_id


def test_bound_policy_changes_and_foreign_lookup_requests_reject(tmp_path):
    _bundle, manifest, rows, records = source(tmp_path / "source", delay=1)
    replay = NativeObservationReplay(manifest, records, decision_latency_ms=1)
    requirements, policies = bind(replay, rows)
    with pytest.raises(ValidationError, match="changed"):
        bind(replay, rows, age=300000)
    with pytest.raises(ValidationError):
        replay.latest_sample(
            BTC,
            "open_interest",
            BTC.key + ":open_interest_history",
            event_before_ms=120000,
            as_of_ms=120001,
            max_age_ms=300000,
        )
    with pytest.raises(ValidationError):
        replay.get(BTC, "depth", BTC.key + ":binance_depth_ws_v1", 60000, as_of_ms=120000)
    selected = prepare(replay, rows[0], requirements)
    assert selected.frame.as_of_ms == 120001
    assert all(pin.observation.available_at_ms <= 120001 for pin in selected.pins)
    native_sample = replay.latest_sample(
        BTC,
        "open_interest",
        BTC.key + ":open_interest_history",
        event_before_ms=120000,
        as_of_ms=120001,
        max_age_ms=330000,
    )
    assert native_sample == next(
        pin for pin in selected.pins if pin.observation.channel == "open_interest"
    )
    assert native_sample is not next(
        pin for pin in selected.pins if pin.observation.channel == "open_interest"
    )
    replay.validate_configuration(requirements, policies)


@pytest.mark.parametrize(
    "age,latency,quality,pin_count",
    [
        (120000, 0, "carried", 2),
        (120000, 1, "missing", 2),
        (119999, 0, "missing", 0),
        (0, 0, "missing", 0),
    ],
)
def test_metric_exact_age_latency_staleness_and_disabled_policy_preserve_pins(
    tmp_path, age, latency, quality, pin_count
):
    _bundle, manifest, rows, records = source(tmp_path / "source")
    replay = NativeObservationReplay(manifest, records, decision_latency_ms=latency)
    requirements, _ = bind(replay, rows, age=age)
    value = prepare(replay, rows[0], requirements)
    metric = value.frame.input("metric")
    assert metric.quality == quality
    native = tuple(
        pin
        for pin in value.pins
        if pin.observation.channel in {"open_interest", "long_short_ratio"}
    )
    assert len(native) == pin_count
    assert all(pin.observation.available_at_ms == 0 for pin in native)
    assert not value.retrospective


def test_native_event_at_close_is_not_selected_until_a_later_minute(tmp_path):
    _bundle, manifest, rows, records = source(tmp_path / "source")
    replay = NativeObservationReplay(manifest, records)
    requirements, _ = bind(replay, rows)
    at_close = prepare(replay, rows[3], requirements)
    after_close = prepare(replay, rows[4], requirements)
    assert (
        next(
            pin for pin in at_close.pins if pin.observation.channel == "open_interest"
        ).observation.event_time_ms
        == 0
    )
    assert (
        next(
            pin for pin in after_close.pins if pin.observation.channel == "open_interest"
        ).observation.event_time_ms
        == 300000
    )


class NativeConsumer:
    descriptor = ModuleDescriptor("native_v2_fixture", "1", frozenset(FIELDS), True)

    def __init__(self):
        self.cursor = None

    def checkpoint(self):
        return {"cursor": self.cursor}

    def restore(self, state):
        self.cursor = state["cursor"]

    def resume(self):
        pass

    def evaluate_protection(self, context):
        self.cursor = context.closed_candle.open_time_ms
        if context.position.quantity and context.closed_candle.low <= 95:
            return TradeDecision(
                (OrderProposal(Side.SELL, abs(context.position.quantity), reducing=True),),
                "local_stop",
            )
        return TradeDecision(reason="local_hold")

    def evaluate(self, context):
        self.cursor = context.closed_candle.open_time_ms
        if (
            not context.position.quantity
            and context.entries_allowed
            and all(context.features[field] is not None for field in FIELDS)
        ):
            return TradeDecision((OrderProposal(Side.BUY, D(1)),), "native_v2_entry")
        return self.evaluate_protection(context)


class Registry:
    def create(self, module, instrument, configuration):
        return (
            NativeConsumer()
            if module == "native_v2_fixture"
            else StrategyRegistry().create(module, instrument, configuration)
        )


def configuration(start=MINUTE):
    return {
        "account": {
            "account_id": "native-v2",
            "balance": "10000",
            "positions": [
                {
                    "instrument": BTC.key,
                    "owner_id": "strategy",
                    "margin_mode": "cross",
                    "leverage": 2,
                    "budget_margin": "100",
                }
            ],
        },
        "strategies": [
            {
                "id": "strategy",
                "instrument": BTC.key,
                "module": "native_v2_fixture",
                "configuration": {},
                "analysis": {
                    "order_flow": [
                        {"interval_id": 0, "analysis_code": code, "nSamples": 2}
                        for code in ("NES", "WOI")
                    ],
                    "market_sentiment": [
                        {"interval_id": 0, "nSamples": 2, "maximum_metric_age_ms": 330000}
                    ],
                },
                "constraints": {"leverage": "2", "can_short": True},
            }
        ],
        "start_ms": start,
        "brackets": [
            {
                "instrument": BTC.key,
                "observed_at_ms": 0,
                "available_at_ms": 0,
                "rows": [
                    {
                        "notional_floor": "0",
                        "notional_cap": "1000000",
                        "maintenance_rate": ".01",
                        "maintenance_amount": "0",
                        "max_leverage": 20,
                    }
                ],
            }
        ],
    }


def preflight(account, strategies, start_ms, **kwargs):
    _, _, rows, _, graphs, _ = prepare_margin_configuration(
        account, strategies, start_ms, registry=Registry(), **kwargs
    )
    required, policies = {}, {}
    for row in rows:
        graph = graphs[row["id"]]
        channels = tuple(sorted(graph.descriptor.required_fields - {"OHLCV"}))
        if channels:
            required[row["instrument"].key] = channels
        if "metric" in channels:
            policies[row["instrument"].key] = MetricInputPolicy(
                graph.maximum_metric_age_ms
            ).document()
    return {"required_channels": required, "metric_policies": policies}


def replay(root, manifest, rows, records, *, settings=None, latency=0, factory=None, **options):
    settings = configuration(rows[0].open_time_ms) if settings is None else settings
    return simulate_margin(
        rows,
        root=root,
        account=settings["account"],
        strategies=settings["strategies"],
        start_ms=settings["start_ms"],
        brackets=scenario_brackets(settings["brackets"]),
        session_factory=factory
        or (lambda *args, **kwargs: MarginTradingSession(*args, registry=Registry(), **kwargs)),
        auxiliary_replay=NativeObservationReplay(manifest, records, decision_latency_ms=latency),
        analysis_preflight=preflight,
        **options,
    )


@pytest.mark.parametrize("latency,expected", [(0, 240000), (1, 300000)])
def test_real_financial_replay_latency_protection_and_cancel_restart(tmp_path, latency, expected):
    _bundle, manifest, rows, records = source(
        tmp_path / "source", count=8, missing_after=3, delay=latency
    )
    root = tmp_path / "replay"

    def stop(progress):
        if progress["completed_frames"] == 3:
            raise Cancelled("explicit cancellation after original input commit")

    with pytest.raises(Cancelled):
        replay(root, manifest, rows, records, latency=latency, progress=stop)
    before = {
        p.relative_to(root).as_posix(): sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }
    with pytest.raises((ProtocolError, ValidationError)):
        replay(root, manifest, rows, records, latency=latency + 1, resume=True)
    assert {
        p.relative_to(root).as_posix(): sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    } == before
    result = replay(root, manifest, rows, records, latency=latency, resume=True)
    assert [D(fill["quantity"]) for fill in result["fills"]] == [D(1), D(-1)]
    assert result["fills"][0]["timestamp_ms"] == expected
    reducing_issue = result["fills"][0]["timestamp_ms"] + MINUTE + latency
    reducing_eligible = (reducing_issue + MINUTE - 1) // MINUTE * MINUTE
    assert result["fills"][1]["timestamp_ms"] == reducing_eligible
    assert result["positions"][BTC.key] == "0"
    continuous = replay(tmp_path / "continuous", manifest, rows, records, latency=latency)
    assert result["fills"] == continuous["fills"] and result["wallet"] == continuous["wallet"]


def test_missing_or_invalid_preflight_creates_no_financial_namespace(tmp_path):
    _bundle, manifest, rows, records = source(tmp_path / "source")
    settings = configuration()
    for index, callback in enumerate(
        (None, lambda *args, **kwargs: {"required_channels": {}, "metric_policies": {BTC.key: {}}})
    ):
        root = tmp_path / str(index)
        with pytest.raises(ValidationError):
            simulate_margin(
                rows,
                root=root,
                account=settings["account"],
                strategies=settings["strategies"],
                start_ms=settings["start_ms"],
                brackets=scenario_brackets(settings["brackets"]),
                session_factory=lambda *args, **kwargs: pytest.fail(
                    "financial owner must not be created"
                ),
                auxiliary_replay=NativeObservationReplay(manifest, records),
                analysis_preflight=callback,
            )
        assert not root.exists()


def test_actual_graph_preflight_runs_once_before_claim_and_financial_factory(tmp_path, monkeypatch):
    import omega.research.margin_simulation as simulation

    _bundle, manifest, rows, records = source(tmp_path / "source")
    settings, calls = configuration(), []
    native = NativeObservationReplay(manifest, records)
    original_claim = simulation._claim

    def actual_preflight(*args, **kwargs):
        calls.append("preflight")
        return preflight(*args, **kwargs)

    def claim(*args, **kwargs):
        calls.append("claim")
        assert (
            native.lineage["graph_configuration"]["metric_policies"][BTC.key]
            == MetricInputPolicy(330000).document()
        )
        return original_claim(*args, **kwargs)

    def factory(*args, **kwargs):
        calls.append("factory")
        return MarginTradingSession(*args, registry=Registry(), **kwargs)

    monkeypatch.setattr(simulation, "_claim", claim)
    simulate_margin(
        rows,
        root=tmp_path / "replay",
        account=settings["account"],
        strategies=settings["strategies"],
        start_ms=settings["start_ms"],
        brackets=scenario_brackets(settings["brackets"]),
        session_factory=factory,
        auxiliary_replay=native,
        analysis_preflight=actual_preflight,
    )
    assert calls == ["preflight", "claim", "factory"]


def test_native_cutoff_cost_ceiling_and_scenario_funding_keep_original_financial_timing(tmp_path):
    _bundle, manifest, rows, records = source(tmp_path / "source", count=8, delay=1)
    funding = {
        "schema_version": 1,
        "model": "linear_usdt_scenario_rate_v1",
        "points": [
            {
                "instrument": BTC.key,
                "event_time_ms": 360000,
                "source_available_at_ms": 361000,
                "reference_time_ms": 360000,
                "rate": ".01",
                "reference_price": "100",
            }
        ],
    }
    result = replay(
        tmp_path / "replay",
        manifest,
        rows,
        records,
        latency=1,
        funding=funding,
        cost_policy=policy(entry_delay_ms=1, slippage_rate="0"),
    )
    assert result["fills"][0]["timestamp_ms"] == 300000
    assert result["funding"]["settlements"] == 1
    assert D(result["funding"]["funding_net"]) == -1
    assert result["funding_history"][0]["event_time_ms"] == 360000


def test_v1_never_invokes_v2_preflight_or_changes_original_lineage(tmp_path):
    _bundle, _manifest, rows, records = source(tmp_path / "source", channels=("trade_flow",))
    bundle = tmp_path / "v1"
    export_auxiliary_dataset(tmp_path / "source/candles", bundle, records)
    manifest, rows, selected = load_auxiliary_dataset(bundle)
    native = NativeObservationReplay(manifest, selected)
    assert native.version == 1
    with pytest.raises(ValidationError, match="version1"):
        native.latest_sample(
            BTC,
            "open_interest",
            BTC.key + ":open_interest_history",
            event_before_ms=120000,
            as_of_ms=120000,
        )
    assert set(native.lineage) == {
        "schema_id",
        "scope",
        "manifest_sha256",
        "candle_manifest_sha256",
        "candle_rows_sha256",
        "observations_sha256",
        "observations_count",
        "selection_policy",
        "declared_trade_flow_sources",
        "decision_latency_ms",
    }
    settings = configuration()
    settings["strategies"][0].update(
        module="target_quantity", configuration={"quantity": "1"}, analysis={}
    )
    arguments = dict(
        account=settings["account"],
        strategies=settings["strategies"],
        start_ms=settings["start_ms"],
        brackets=scenario_brackets(settings["brackets"]),
        session_factory=MarginTradingSession,
        auxiliary_replay=native,
    )
    first = simulate_margin(
        rows,
        root=tmp_path / "first",
        analysis_preflight=lambda *args, **kwargs: pytest.fail("V1 preflight changed"),
        **arguments,
    )
    second = simulate_margin(rows, root=tmp_path / "second", **arguments)
    assert (
        first["lineage"] == second["lineage"]
        and first["auxiliary_replay"] == second["auxiliary_replay"]
    )
    assert first["fills"] == second["fills"] and first["wallet"] == second["wallet"]


def test_builtin_job_uses_same_snapshot_and_results_without_promoting_native_history(tmp_path):
    bundle, _manifest, rows, _records = source(tmp_path / "source")
    settings = configuration()
    settings["strategies"][0].update(
        module="target_quantity", configuration={"quantity": "1"}, analysis={}
    )
    partial = tmp_path / "partial"
    partial.mkdir()
    response = run_margin_simulation_job(
        partial,
        tmp_path / "final",
        settings
        | {
            "dataset": str(bundle),
            "auxiliary_replay": {"mode": "native", "decision_latency_ms": 0},
        },
        None,
    )
    result = json.loads((partial / response["result_file"]).read_text())
    assert result["input_manifest"]["schema_id"] == "omega.market-observations.1m.v2"
    assert any(
        "original retained-native trade-flow/depth/OI/long-short" in value
        for value in result["assumptions"]
    )
    assert not any(
        "selected-normalized trade-flow observations only" in value
        for value in result["assumptions"]
    )
    assert result["auxiliary_replay"]["schema_id"] == "omega.native-margin-observation-replay.v2"
    assert result["fills"] and result["fills"][0]["timestamp_ms"] == rows[1].open_time_ms
    assert (partial / "input-dataset/observations.jsonl").is_file()
    assert (
        run_margin_simulation_job.omega_feature_scopes["auxiliary_replay"]
        == "native_trade_flow_available_cutoff_v1"
    )


@pytest.mark.parametrize("model,count", [("CGALPHA", 1442), ("PATHFINDER", 242)])
def test_complete_builtin_fields_warm_and_produce_actual_virtual_fills(tmp_path, model, count):
    """Both use shorter valid user periods; this is not default-model/Eta parity."""
    fixture = (
        Path(__file__).parents[1]
        / "fixtures"
        / (
            "tef-cgalpha-native-inputs.json"
            if model == "CGALPHA"
            else "tef-pathfinder-native-inputs.json"
        )
    )
    profile = json.loads(fixture.read_text())
    channels = ("trade_flow", "depth") if model == "CGALPHA" else tuple(SUFFIXES)
    _bundle, manifest, rows, records = source(
        tmp_path / "source", count=count, start=0, channels=channels, varied=True
    )
    settings = configuration(0)
    if rows[-1].open_time_ms + MINUTE > 86400000:
        settings["brackets"].append(
            settings["brackets"][0] | {"observed_at_ms": 86400000, "available_at_ms": 86400000}
        )
    strategy = settings["strategies"][0]
    strategy.update(
        module="tef", configuration=profile["strategy_configuration"], analysis=profile["analysis"]
    )
    if model == "CGALPHA":
        averages = strategy["analysis"]["moving_averages"][0]
        averages.update(mmacd_periods=[2, 3], signal_period=2)
        for group in ("momentum", "order_flow"):
            for item in strategy["analysis"][group]:
                item["nSamples"] = 2
        strategy["configuration"]["parameters"] = {
            f"{scale}_Beta_{side}_{action}": 0.0
            for scale in ("MTF", "STF")
            for side in ("S", "L")
            for action in ("Entry", "Exit")
        }
    else:
        strategy["analysis"]["market_sentiment"][0]["nSamples"] = 2
        strategy["analysis"]["short_term_scalar"][0].update(
            MMACDFast=2,
            MMACDSlow=3,
            MMACDSignal=2,
            DMIxADXNSamples=2,
            MFINSamples=2,
            WOINSamples=2,
            WOIBaseNSamples=2,
        )
        strategy["configuration"]["parameters"] = {
            "MST_Deadzone": 0.0,
            "MST_Bias": 1.0,
            "Acc_Threshold_S": 0.01,
            "Acc_Threshold_L": 0.01,
        }
    sessions = []

    def factory(*args, **kwargs):
        session = MarginTradingSession(*args, **kwargs)
        sessions.append(session)
        return session

    result = replay(
        tmp_path / "replay", manifest, rows, records, settings=settings, factory=factory
    )
    last = sessions[0]._meta["last_frame"]["features"]["strategy"]
    required = set(profile["required_fields"])
    assert required <= last.keys()
    assert all(last[field] is not None for field in required if not field.startswith("CLOSED_"))
    assert result["fills"], "A priming-only/all-held run is not accepted as a warmed builtin replay"
    assert result["auxiliary_replay"]["graph_configuration"]["required_channels"][
        BTC.key
    ] == sorted(set(sessions[0]._graphs["strategy"].descriptor.required_fields) - {"OHLCV"})
