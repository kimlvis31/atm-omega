"""Real virtual owner/native depth + flow; no DB, network or real account.

The dual-input fixture proves financial behavior; CGALPHA separately proves its
complete real graph can receive both inputs while honest numeric warmup holds.
"""

import json
from decimal import Decimal as D
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

from omega.analysis.order_flow import DEPTH_FIELDS
from omega.application.auxiliary_codec import (
    decode_observation,
    encode_observation,
    observation_checksum,
)
from omega.application.market_frame_builder import MarketFrameBuilder
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market, Side
from omega.domain.trading import OrderProposal, TradeDecision
from omega.infrastructure.binance.depth_sampling import DepthMinuteSampler
from omega.ports.analysis_inputs import PreparedMarketFrame
from omega.ports.auxiliary import StoredAuxiliary
from omega.ports.modules import ModuleDescriptor
from omega.runtime.margin_console import scenario_brackets
from omega.runtime.margin_session import MarginTradingSession
from omega.runtime.margin_session_inputs import MarginSessionInputs
from omega.runtime.margin_trading import MarginTradingService

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


class DualConsumer:
    descriptor = ModuleDescriptor(
        "dual_depth_fixture", "1", frozenset({"0_NES_0_NES", "0_WOI_0_WOI"}), True
    )

    def __init__(self):
        self.cursor = None

    def checkpoint(self):
        return {"cursor": self.cursor}

    def restore(self, document):
        self.cursor = document["cursor"]

    def resume(self):
        pass

    def evaluate_protection(self, context):
        self.cursor = context.closed_candle.open_time_ms
        if context.position.quantity and context.closed_candle.low <= D(95):
            return TradeDecision(
                (OrderProposal(Side.SELL, abs(context.position.quantity), reducing=True),),
                "synthetic_stop_without_depth",
            )
        return TradeDecision(reason="synthetic_hold")

    def evaluate(self, context):
        self.cursor = context.closed_candle.open_time_ms
        values = [context.features[name] for name in self.descriptor.required_fields]
        if (
            all(value is not None and value > 0 for value in values)
            and not context.position.quantity
            and context.entries_allowed
        ):
            return TradeDecision((OrderProposal(Side.BUY, D(1)),), "synthetic_dual_entry")
        return self.evaluate_protection(context)


class Registry:
    def create(self, module, _instrument, _configuration):
        assert module == "dual_depth_fixture"
        return DualConsumer()


class PinStore:
    def __init__(self, records):
        self.records = records

    def get(self, instrument, channel, source, opening, *, as_of_ms):
        return next(
            (
                row
                for row in self.records
                if row.instrument == instrument
                and row.observation.channel == channel
                and row.observation.source == source
                and row.observation.window_start_ms == opening
                and row.observation.available_at_ms <= as_of_ms
            ),
            None,
        )


def bar(index, *, low="99"):
    return Candle(BTC, index * 60000, (index + 1) * 60000, D(100), D(101), D(low), D(100), D(10))


def pin(observation):
    return StoredAuxiliary(
        BTC, observation, 1, observation_checksum(encode_observation(BTC, observation)), None
    )


def prepared(index, *, missing_depth=False, low="99"):
    candle = bar(index, low=low)
    flow = AuxiliaryInput(
        "trade_flow",
        (("AGGTRADE_NOTIONALBUY", D(1600)), ("AGGTRADE_NOTIONALSELL", D(500))),
        candle.open_time_ms,
        candle.close_time_ms,
        candle.close_time_ms,
        candle.close_time_ms,
        "observed",
        D(1),
        BTC.key + ":aggTrade",
    )
    if missing_depth:
        depth = (
            DepthMinuteSampler(BTC, price_precision=2, quote_precision=8)
            .finalize(candle.open_time_ms, as_of_ms=candle.close_time_ms)
            .observation
        )
    else:
        depth = AuxiliaryInput(
            "depth",
            tuple((name, D(200) if "BIDS" in name else D(100)) for name in DEPTH_FIELDS),
            candle.open_time_ms,
            candle.close_time_ms,
            candle.close_time_ms - 1,
            candle.close_time_ms,
            "observed",
            D(1),
            BTC.key + ":binance_depth_ws_v1",
        )
    return MarketFrameBuilder(PinStore((pin(flow), pin(depth)))).prepare(
        candle, as_of_ms=candle.close_time_ms, channels=("depth", "trade_flow")
    )


def config(*, cgalpha=False):
    fixture = json.loads((FIXTURES / "tef-cgalpha-native-inputs.json").read_text(encoding="utf-8"))
    account = {
        "account_id": "depth-fixture",
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
    }
    strategy = {
        "id": "flow",
        "instrument": BTC.key,
        "module": "tef" if cgalpha else "dual_depth_fixture",
        "configuration": fixture["strategy_configuration"] if cgalpha else {},
        "analysis": fixture["analysis"]
        if cgalpha
        else {
            "order_flow": [
                {"interval_id": 0, "analysis_code": name, "nSamples": 2} for name in ("NES", "WOI")
            ]
        },
        "constraints": {"leverage": "10", "can_short": True},
    }
    brackets = scenario_brackets(
        [
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
        ]
    )
    return account, [strategy], brackets, fixture


def test_original_actual781_flow_document_and_status_are_byte_compatible():
    fixture = json.loads((FIXTURES / "margin-native-flow-v1.json").read_text(encoding="utf-8"))
    assert (
        sha256(json.dumps(fixture, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        == "1d078c04f21fda72cd6c412ced334d6096f99d1aef2cda8b3a7c21f9c21a51de"
    )
    reader = MarginSessionInputs(
        [{"id": "legacy", "instrument": BTC}],
        {
            "legacy": SimpleNamespace(
                descriptor=SimpleNamespace(required_fields={"OHLCV", "trade_flow"})
            )
        },
    )
    candle = Candle(BTC, 0, 60000, D(100), D(110), D(90), D(101), D(10))
    for name, document in fixture["cases"].items():
        restored = reader.restore(
            document, (candle,), as_of_ms=60000, expected=fixture["destination"]
        )
        pins = []
        for row in document["frames"][0]["pins"]:
            instrument, observation = decode_observation(row["payload"].encode())
            pins.append(
                StoredAuxiliary(
                    instrument,
                    observation,
                    row["revision"],
                    row["checksum"],
                    row["previous_checksum"],
                )
            )
        copied, _ = reader.freeze(
            (PreparedMarketFrame(restored[BTC.key], tuple(pins)),),
            (candle,),
            as_of_ms=60000,
            destination=fixture["destination"],
        )
        assert copied == document and reader.status(copied) == fixture["statuses"][name]


def test_actual_woi_nes_entry_then_missing_depth_keeps_fill_and_local_protection(tmp_path):
    account, strategies, brackets, _ = config()
    session = MarginTradingSession(
        tmp_path, account, strategies, 0, registry=Registry(), create=True
    )
    try:
        assert session.analysis_input_channels == {BTC.key: ("depth", "trade_flow")}
        session.resume("explicit_fixture_resume")
        for index in range(4):
            value = prepared(index, missing_depth=index >= 2, low="90" if index == 2 else "99")
            view = session.run_once(
                (value.frame.candle,),
                as_of_ms=value.frame.as_of_ms,
                brackets=brackets,
                prepared_frames=(value,),
                auxiliary_destination="d" * 64,
            )
            assert view["graph_errors"] == {}
            if index == 1:
                assert session._meta["last_frame"]["features"]["flow"]["0_WOI_0_WOI"] > 0
                assert session._meta["last_frame"]["features"]["flow"]["0_NES_0_NES"] > 0
                assert view["pending_orders"]
            if index == 2:
                assert view["market_inputs"]["state"] == "unavailable"
                assert len(session.execution.snapshot().account.ledger) == 1
                assert view["pending_orders"]  # Original independent reduction was admitted.
        ledger = session.execution.snapshot().account.ledger
        assert [entry.fact.delta_qty for entry in ledger] == [D(1), D(-1)]
        assert [entry.fact.event_time_ms for entry in ledger] == [120000, 180000]
        assert session.execution.snapshot().account.positions[0].quantity == 0
    finally:
        session.close()


def test_real_cgalpha_complete_graph_accepts_original_depth_and_flow_during_warmup(tmp_path):
    account, strategies, brackets, fixture = config(cgalpha=True)
    session = MarginTradingSession(tmp_path, account, strategies, 0, create=True)
    try:
        assert session._modules["flow"].descriptor.required_fields == frozenset(
            fixture["required_fields"]
        )
        assert session.analysis_input_channels == {BTC.key: ("depth", "trade_flow")}
        session.resume("explicit_cgalpha_fixture_resume")
        value = prepared(0)
        view = session.run_once(
            (value.frame.candle,),
            as_of_ms=60000,
            brackets=brackets,
            prepared_frames=(value,),
            auxiliary_destination="e" * 64,
        )
        assert view["graph_errors"] == {} and view["state"] == "ready"
        assert view["market_inputs"]["state"] == "ready"
        features = session._meta["last_frame"]["features"]["flow"]
        # Input quality and model numeric/history readiness are separate contracts.
        assert features["CLOSED_7"] is False
        assert any(features[name] is None for name in fixture["required_fields"])
        assert session._modules["flow"].history_priming_ready is False
        assert set(fixture["required_fields"]) <= features.keys()
        assert not view["pending_orders"] and not session.execution.snapshot().account.ledger
    finally:
        session.close()


def test_runtime_fetch_forwards_both_canonical_native_channels_without_metric_fabrication():
    value = prepared(0)
    calls = []

    def read_frames(*args, **kwargs):
        calls.append((args, kwargs))
        return (value,)

    runtime = object.__new__(MarginTradingService)
    runtime.source = SimpleNamespace(destination_id="a" * 64, read_frames=read_frames)
    session = SimpleNamespace(analysis_input_channels={BTC.key: ("depth", "trade_flow")})
    options, reason = runtime._analysis_inputs(session, {}, (value.frame.candle,), as_of_ms=60000)
    assert reason is None and options["prepared_frames"] == (value,)
    assert calls[0][1]["channels"] == ("depth", "trade_flow")
    assert calls[0][1]["maximum_metric_age_ms"] == 0
