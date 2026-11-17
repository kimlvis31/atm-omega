"""File-backed native-input replay: synthetic observations, real financial owner.

The test-only NES consumer verifies the TS port; it is not built-in CGALPHA parity.
The composed builtin job separately verifies the exported input/result workflow.
"""

import json
from decimal import Decimal as D
from hashlib import sha256

import pytest
from test_margin_cost_research import policy

from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market, Side
from omega.domain.trading import OrderProposal, TradeDecision
from omega.ports.auxiliary import StoredAuxiliary
from omega.ports.modules import ModuleDescriptor
from omega.research.artifacts import Cancelled
from omega.research.auxiliary_datasets import export_auxiliary_dataset, load_auxiliary_dataset
from omega.research.datasets import MarketRow, export_dataset, write_rows
from omega.research.margin_observation_inputs import NativeObservationReplay
from omega.research.margin_simulation import simulate_margin
from omega.research.result_summary import summarize_simulation
from omega.runtime.margin_console import scenario_brackets
from omega.runtime.margin_research import run_margin_simulation_job
from omega.runtime.margin_session import MarginTradingSession

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
FIELD = "0_NES_0_NES"


class NESConsumer:
    descriptor = ModuleDescriptor("research_nes_fixture", "1", frozenset({FIELD}), True)

    def __init__(self):
        self.cursor = None

    def checkpoint(self):
        return {"cursor": self.cursor}

    def restore(self, value):
        self.cursor = value["cursor"]

    def resume(self):
        pass

    def evaluate_protection(self, context):
        self.cursor = context.closed_candle.open_time_ms
        if context.position.quantity and context.closed_candle.low <= D(95):
            return TradeDecision(
                (OrderProposal(Side.SELL, abs(context.position.quantity), reducing=True),),
                "synthetic_local_stop",
            )
        return TradeDecision(reason="synthetic_hold")

    def evaluate(self, context):
        self.cursor = context.closed_candle.open_time_ms
        if (
            context.features[FIELD] is not None
            and context.features[FIELD] > 0
            and not context.position.quantity
            and context.entries_allowed
        ):
            return TradeDecision((OrderProposal(Side.BUY, D(1)),), "synthetic_nes_entry")
        return self.evaluate_protection(context)


class Registry:
    def create(self, module, _instrument, _configuration):
        assert module == "research_nes_fixture"
        return NESConsumer()


def owner(*args, **kwargs):
    return MarginTradingSession(*args, registry=Registry(), **kwargs)


def source_bundle(root, *, received_delay=0, missing_after=None, low=D(99), count=6):
    root.mkdir()
    rows = [MarketRow(BTC.key, i * 60000, D(100), D(101), low, D(100), D(10)) for i in range(count)]
    csv = root / "source.csv"
    write_rows(csv, rows)
    candles = root / "candles"
    export_dataset(csv, candles, {})
    records = []
    for index, row in enumerate(rows):
        if missing_after is not None and index >= missing_after:
            continue
        observation = AuxiliaryInput(
            "trade_flow",
            (
                ("AGGTRADE_QUANTITYBUY", D(8)),
                ("AGGTRADE_QUANTITYSELL", D(2)),
                ("AGGTRADE_NOTIONALBUY", D(800)),
                ("AGGTRADE_NOTIONALSELL", D(200)),
                ("AGGTRADE_NTRADESBUY", 4),
                ("AGGTRADE_NTRADESSELL", 1),
            ),
            row.open_time_ms,
            row.open_time_ms + 60000,
            row.open_time_ms + 60000,
            row.open_time_ms + 60000 + received_delay,
            "observed",
            D(1),
            BTC.key + ":aggTrade",
        )
        records.append(
            StoredAuxiliary(
                BTC,
                observation,
                1,
                observation_checksum(encode_observation(BTC, observation)),
                None,
            )
        )
    bundle = root / "observations"
    export_auxiliary_dataset(candles, bundle, records)
    manifest, restored_rows, selected = load_auxiliary_dataset(bundle)
    return bundle, manifest, restored_rows, selected


def configuration(*, builtin=False):
    return {
        "account": {
            "account_id": "native-replay-fixture",
            "balance": "1000",
            "positions": [
                {
                    "instrument": BTC.key,
                    "owner_id": "flow",
                    "margin_mode": "cross",
                    "leverage": 10,
                    "budget_margin": "100",
                }
            ],
        },
        "strategies": [
            {
                "id": "flow",
                "instrument": BTC.key,
                "module": "target_quantity" if builtin else "research_nes_fixture",
                "configuration": {"quantity": "1"} if builtin else {},
                "analysis": {}
                if builtin
                else {"order_flow": [{"interval_id": 0, "analysis_code": "NES", "nSamples": 2}]},
                "constraints": {"leverage": "10", "can_short": True},
            }
        ],
        "start_ms": 0,
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


def replay(root, manifest, rows, records, *, latency=0, **options):
    configuration_value = configuration()
    return simulate_margin(
        rows,
        root=root,
        account=configuration_value["account"],
        strategies=configuration_value["strategies"],
        start_ms=0,
        brackets=scenario_brackets(configuration_value["brackets"]),
        session_factory=owner,
        auxiliary_replay=NativeObservationReplay(manifest, records, decision_latency_ms=latency),
        **options,
    )


def file_hashes(root):
    return {
        p.relative_to(root).as_posix(): sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("latency,expected", [(0, 120000), (1, 180000), (60000, 180000)])
def test_actual_nes_decision_uses_cutoff_and_cannot_fill_at_past_open(tmp_path, latency, expected):
    bundle, manifest, rows, records = source_bundle(tmp_path / "source", received_delay=latency)
    original = file_hashes(bundle)
    result = replay(tmp_path / "replay", manifest, rows, records, latency=latency)
    assert result["fills"] and result["fills"][0]["timestamp_ms"] == expected
    assert result["fills"][0]["available_at_ms"] == expected + 60000 + latency
    assert result["equity"][-1]["timestamp_ms"] == 360000 + latency
    assert result["equity"][-1]["market_close_time_ms"] == 360000
    assert result["equity"][-1]["wallet"] == result["wallet"]
    assert summarize_simulation(result)["metrics"]["initial_wallet"] == "1000"
    assert result["auxiliary_replay"]["decision_latency_ms"] == latency
    assert file_hashes(bundle) == original


def test_late_records_stay_missing_instead_of_backdated_into_a_decision(tmp_path):
    _bundle, manifest, rows, records = source_bundle(tmp_path / "source", received_delay=2)
    result = replay(tmp_path / "replay", manifest, rows, records, latency=1)
    assert result["fills"] == [] and result["pending_orders"] == 0
    assert all(row["reason"] != "synthetic_nes_entry" for row in result["strategies"])


def test_missing_following_native_input_keeps_original_fill_and_local_reduction(tmp_path):
    _bundle, manifest, rows, records = source_bundle(
        tmp_path / "source", missing_after=2, low=D(90)
    )
    result = replay(tmp_path / "replay", manifest, rows, records)
    assert [D(row["quantity"]) for row in result["fills"]] == [D(1), D(-1)]
    assert [row["timestamp_ms"] for row in result["fills"]] == [120000, 180000]
    assert result["positions"][BTC.key] == "0" and result["pending_orders"] == 0


def test_selected_cost_delay_is_added_to_original_delayed_issue_once(tmp_path):
    _bundle, manifest, rows, records = source_bundle(tmp_path / "source", received_delay=60000)
    result = replay(
        tmp_path / "replay",
        manifest,
        rows,
        records,
        latency=60000,
        cost_policy=policy(entry_delay_ms=1, slippage_rate="0"),
    )
    assert result["fills"][0]["timestamp_ms"] == 240000
    assert len(result["fills"]) == 1


def test_cancel_resume_preserves_frozen_inputs_and_changed_latency_cannot_reopen(tmp_path):
    _bundle, manifest, rows, records = source_bundle(tmp_path / "source", received_delay=1)
    root = tmp_path / "replay"

    def cancel_after_two(progress):
        if progress["completed_frames"] == 2:
            raise Cancelled("synthetic user cancellation after durable frame")

    with pytest.raises(Cancelled):
        replay(root, manifest, rows, records, latency=1, progress=cancel_after_two)
    before = file_hashes(root)
    with pytest.raises(ProtocolError):
        replay(root, manifest, rows, records, latency=2, resume=True)
    assert file_hashes(root) == before
    resumed = replay(root, manifest, rows, records, latency=1, resume=True)
    continuous = replay(tmp_path / "continuous", manifest, rows, records, latency=1)
    assert resumed["fills"] == continuous["fills"]
    assert resumed["wallet"] == continuous["wallet"]


def test_composed_builtin_job_snapshots_bundle_and_publishes_existing_result_format(tmp_path):
    bundle, _manifest, _rows, _records = source_bundle(tmp_path / "source", received_delay=1)
    original = file_hashes(bundle)
    parameters = configuration(builtin=True) | {
        "dataset": str(bundle),
        "auxiliary_replay": {"mode": "native", "decision_latency_ms": 1},
    }
    partial = tmp_path / "partial"
    partial.mkdir()
    response = run_margin_simulation_job(partial, tmp_path / "final", parameters, None)
    result = json.loads((partial / response["result_file"]).read_text(encoding="utf-8"))
    assert result["fills"] and result["scope"] == "virtual_margin_v2_ohlcv_proxy"
    assert response["summary"]["input_scope"] == "native_observation_replay"
    assert result["input_manifest"]["schema_id"] == "omega.market-observations.1m.v1"
    assert (partial / "input-dataset/observations.jsonl").is_file()
    assert summarize_simulation(result)["metrics"]["initial_wallet"] == "1000"
    assert file_hashes(bundle) == original


def test_funding_keeps_original_settlement_minute_with_delayed_native_decision(tmp_path):
    _bundle, manifest, rows, records = source_bundle(tmp_path / "source", received_delay=1)
    funding = {
        "schema_version": 1,
        "model": "linear_usdt_scenario_rate_v1",
        "points": [
            {
                "instrument": BTC.key,
                "event_time_ms": 240000,
                "source_available_at_ms": 241000,
                "reference_time_ms": 240000,
                "rate": ".01",
                "reference_price": "100",
            }
        ],
    }
    result = replay(tmp_path / "replay", manifest, rows, records, latency=1, funding=funding)
    assert result["fills"][0]["timestamp_ms"] == 180000
    assert D(result["funding"]["funding_net"]) == D(-1)
    (settlement,) = result["funding_history"]
    assert settlement["timestamp_ms"] == 240000 and settlement["available_at_ms"] == 300001
    assert result["equity"][-1]["wallet"] == result["wallet"]


@pytest.mark.parametrize(
    "policy_value", [None, True, {"mode": "native", "decision_latency_ms": True}]
)
def test_native_policy_on_plain_candles_rejects_before_partial_input_writes(tmp_path, policy_value):
    bundle, _manifest, _rows, _records = source_bundle(tmp_path / "source")
    partial = tmp_path / "partial"
    partial.mkdir()
    parameters = configuration(builtin=True) | {
        "dataset": str(bundle.parent / "candles"),
        "auxiliary_replay": policy_value,
    }
    with pytest.raises(ValidationError):
        run_margin_simulation_job(partial, tmp_path / "final", parameters, None)
    assert list(partial.iterdir()) == []
