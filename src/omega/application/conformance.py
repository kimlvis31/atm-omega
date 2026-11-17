"""Offline TS interface and safety checks. This is a smoke suite, not profitability proof."""

# Imports
import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal

from omega.application.trading import TradingEngine
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market, OrderKind, Side
from omega.domain.trading import (
    BudgetView,
    ExecutionConstraints,
    OrderProposal,
    PositionView,
    TradeContext,
    TradeDecision,
)
from omega.ports.modules import ModuleDescriptor


def context(instrument=None, **changes):
    instrument = instrument or Instrument("binance", Market.USDM, "BTCUSDT")
    candle = Candle(
        instrument, 0, 60_000, Decimal(100), Decimal(101), Decimal(99), Decimal(100), Decimal(10)
    )
    baseline = TradeContext(
        instrument,
        Decimal(100),
        60_000,
        PositionView(),
        BudgetView(Decimal(1000), Decimal(1000)),
        ExecutionConstraints(can_short=True),
        candle,
        {"0_MMACD_MSDELTAABSMAREL": 1.0},
    )
    return replace(baseline, **changes)


def _case(identity, scope, action):
    try:
        action()
        return {"id": identity, "scope": scope, "status": "passed"}
    except Exception as exc:
        return {
            "id": identity,
            "scope": scope,
            "status": "failed",
            "error": type(exc).__name__,
            "detail": str(exc)[:300],
        }


def _require(condition, message):
    if not condition:
        raise AssertionError(message)


class _ProbeModule:
    descriptor = ModuleDescriptor("executor_probe", "1", frozenset())

    def __init__(self, proposal):
        self.proposal = proposal

    def evaluate(self, _context):
        return TradeDecision((self.proposal,))

    def snapshot(self):
        return {}

    def resume(self):
        pass


class _CancellationProbe(_ProbeModule):
    def evaluate(self, _context):
        return TradeDecision(reason="cancel_probe", cancel_pending=True)


def executor_cases(instrument):
    cases = []
    base = context(instrument)
    buy = OrderProposal(Side.BUY, Decimal(1))
    long = PositionView(Decimal(1), Decimal(100), 1)
    reduce = OrderProposal(Side.SELL, Decimal(1), reducing=True)
    examples = [
        ("new_entry_pause", replace(base, entries_allowed=False), buy),
        ("uncertain_position", replace(base, position=long, state_certain=False), reduce),
        ("external_control", replace(base, position=long, control_owned=False), reduce),
        ("pending_order", replace(base, has_pending_order=True), buy),
        (
            "close_before_reverse",
            replace(base, position=long),
            OrderProposal(Side.SELL, Decimal(2)),
        ),
        (
            "cannot_over_reduce",
            replace(base, position=long),
            OrderProposal(Side.SELL, Decimal(2), reducing=True),
        ),
        ("insufficient_budget", replace(base, budget=BudgetView(Decimal(1000), Decimal(0))), buy),
        (
            "spot_cannot_short",
            context(Instrument("binance", Market.SPOT, "BTCUSDT")),
            OrderProposal(Side.SELL, Decimal(1)),
        ),
    ]
    for name, sample, proposal in examples:

        def check(sample=sample, proposal=proposal):
            decision = TradingEngine(_ProbeModule(proposal)).evaluate(sample)
            _require(not decision.orders, "executor accepted a forbidden proposal")

        cases.append(_case(name, "common_executor", check))

    def invalid_step():
        try:
            TradingEngine(_ProbeModule(OrderProposal(Side.BUY, Decimal("0.0001")))).evaluate(base)
        except ValidationError:
            return
        raise AssertionError("off-step quantity was accepted")

    def invalid_tick():
        try:
            proposal = OrderProposal(Side.BUY, Decimal(1), OrderKind.LIMIT, Decimal("100.001"))
            TradingEngine(_ProbeModule(proposal)).evaluate(base)
        except ValidationError:
            return
        raise AssertionError("off-tick price was accepted")

    cases.extend(
        [
            _case("quantity_step", "common_executor", invalid_step),
            _case("price_tick", "common_executor", invalid_tick),
        ]
    )
    for identity, sample, allowed in (
        (
            "cancel_pending_while_entries_paused",
            replace(base, has_pending_order=True, entries_allowed=False),
            True,
        ),
        ("cancel_requires_pending_order", base, False),
        (
            "cancel_requires_certain_state",
            replace(base, has_pending_order=True, state_certain=False),
            False,
        ),
        (
            "cancel_requires_control",
            replace(base, has_pending_order=True, control_owned=False),
            False,
        ),
    ):

        def check(sample=sample, allowed=allowed):
            decision = TradingEngine(_CancellationProbe(buy)).evaluate(sample)
            _require(
                decision.cancel_pending is allowed and not decision.orders,
                "cancellation bypassed its ownership/state/order boundary",
            )

        cases.append(_case(identity, "common_executor", check))
    return cases


def module_cases(factory, instrument):
    cases = []
    base = context(instrument)
    selected = factory()
    fixture = getattr(selected, "validation_features", None)
    if callable(fixture):
        features = fixture()
        if type(features) is not dict or set(features) != set(selected.descriptor.required_fields):
            raise ValidationError("TS validation fixture must cover its exact required fields")
        base = replace(base, features=features)

    def contract():
        module = factory()
        _require(isinstance(module.descriptor, ModuleDescriptor), "missing typed descriptor")
        json.dumps(module.snapshot(), allow_nan=False)
        original = deepcopy(module.snapshot())
        detached = module.snapshot()
        detached["__probe__"] = "changed"
        _require(module.snapshot() == original, "snapshot exposes mutable module state")
        module.resume()
        _require(
            isinstance(module.evaluate(base), TradeDecision), "evaluate must return TradeDecision"
        )

    def replay():
        engine = TradingEngine(factory())
        first = engine.evaluate(base)
        snapshot = deepcopy(engine.module.snapshot())
        _require(engine.evaluate(base) == first, "repeated observation changed its result")
        _require(engine.module.snapshot() == snapshot, "duplicate observation advanced state")
        try:
            engine.evaluate(replace(base, observed_at_ms=0, closed_candle=None))
        except ValidationError:
            return
        raise AssertionError("late observations were not rejected")

    def isolation():
        first, second = factory(), factory()
        before = deepcopy(second.snapshot())
        first.evaluate(base)
        _require(second.snapshot() == before, "module instances share mutable state")

    cases.extend(
        [
            _case("interface_and_snapshot", "selected_module", contract),
            _case("duplicate_and_late_observation", "selected_module", replay),
            _case("instance_state_isolation", "selected_module", isolation),
        ]
    )
    for flag in ("entries_allowed", "state_certain", "control_owned"):

        def check(flag=flag):
            sample = replace(base, **{flag: False})
            decision = TradingEngine(factory()).evaluate(sample)
            _require(not decision.orders, f"selected module escaped {flag} execution gate")

        cases.append(_case(flag, "selected_module_via_executor", check))
    return cases


def validate_tef(factory, instrument):
    cases = []
    long = PositionView(Decimal(1), Decimal(100), 1)

    def warmup():
        decision = factory({"model": "MMACDDEFAULT"}).evaluate(context(instrument, features={}))
        _require(
            not decision.orders and decision.reason == "indicator_warmup",
            "missing features must not become a zero-target exit",
        )

    def long_entry():
        decision = factory({"model": "MMACDDEFAULT"}).evaluate(context(instrument))
        _require(
            len(decision.orders) == 1
            and decision.orders[0].side == Side.BUY
            and not decision.orders[0].reducing,
            "positive MMACD should propose long entry",
        )

    def reversal():
        decision = factory({"model": "MMACDDEFAULT"}).evaluate(
            context(instrument, position=long, features={"0_MMACD_MSDELTAABSMAREL": -1.0})
        )
        _require(
            len(decision.orders) == 1
            and decision.orders[0].side == Side.SELL
            and decision.orders[0].quantity == 1
            and decision.orders[0].reducing,
            "reversal must first close observed long quantity",
        )

    def stop_while_paused():
        module = factory({"model": "MMACDDEFAULT", "fullStopLossImmediate": "0.05"})
        decision = module.evaluate(
            context(
                instrument, price=Decimal(90), position=long, entries_allowed=False, features={}
            )
        )
        _require(
            len(decision.orders) == 1
            and decision.orders[0].reducing
            and decision.reason == "FSLIMMED",
            "local stop must remain available while entries are paused",
        )

    def no_reentry_after_stop():
        module = factory(
            {"model": "MMACDDEFAULT", "fullStopLossImmediate": "0.05", "stopTradeOnFSL": True}
        )
        sample = context(instrument, price=Decimal(90), position=long, features={})
        module.evaluate(sample)
        decision = module.evaluate(
            replace(
                sample, position=PositionView(revision=2), closed_candle=None, observed_at_ms=60_001
            )
        )
        _require(
            not decision.orders and module.snapshot()["halted"],
            "a filled local stop must latch until explicit resume",
        )
        module.resume()
        _require(not module.snapshot()["halted"], "explicit resume did not clear the local latch")

    def wick_stop():
        module = factory({"fullStopLossImmediate": "0.05"})
        sample = context(instrument, position=long, entries_allowed=False, features={})
        sample = replace(sample, closed_candle=replace(sample.closed_candle, low=Decimal(94)))
        decision = module.evaluate(sample)
        _require(
            decision.reason == "FSLIMMED" and decision.orders[0].reducing,
            "a recovered close hid a valid post-entry stop touch",
        )

    def before_entry_wick():
        module = factory({"fullStopLossImmediate": "0.05"})
        sample = context(instrument, position=replace(long, last_changed_ms=1), features={})
        sample = replace(sample, closed_candle=replace(sample.closed_candle, low=Decimal(94)))
        _require(not module.evaluate(sample).orders, "pre-entry wick caused a retroactive stop")

    def protection_cancel():
        module = factory({"fullStopLossImmediate": "0.05"})
        sample = context(
            instrument, price=Decimal(90), position=long, features={}, has_pending_order=True
        )
        cancel = module.evaluate(sample)
        _require(
            cancel.cancel_pending and not cancel.orders,
            "local protection replaced a pending order before confirmed cancellation",
        )
        _require(
            module.snapshot()["pending_stop"] is None, "cancellation was mistaken for a stop fill"
        )
        replacement = module.evaluate(replace(sample, has_pending_order=False))
        _require(
            replacement.orders and replacement.orders[0].reducing,
            "confirmed cancellation did not permit local protection",
        )

    def residual_stop():
        module = factory({"fullStopLossImmediate": "0.05"})
        sample = context(instrument, price=Decimal(90), position=long, features={})
        module.evaluate(sample)
        partial = replace(
            sample,
            price=Decimal(100),
            closed_candle=None,
            observed_at_ms=60_001,
            position=PositionView(Decimal("0.5"), Decimal(100), 2),
        )
        decision = module.evaluate(partial)
        _require(
            decision.orders and all(order.reducing for order in decision.orders),
            "residual local protection increased risk after price recovery",
        )

    def dust_stop():
        module = factory({"fullStopLossImmediate": "0.05"})
        decision = module.evaluate(
            context(
                instrument,
                price=Decimal(90),
                features={},
                position=PositionView(Decimal("0.0005"), Decimal(100), 1),
            )
        )
        _require(
            not decision.orders and module.snapshot()["pending_stop"] is None,
            "an unsubmitted dust stop created a confirmed protection latch",
        )

    def checkpoint_stop():
        configuration = {"fullStopLossImmediate": "0.05"}
        module = factory(configuration)
        sample = context(instrument, price=Decimal(90), position=long, features={})
        module.evaluate(sample)
        candidate = factory(configuration)
        candidate.restore(json.loads(json.dumps(module.checkpoint(), allow_nan=False)))
        later = replace(
            sample,
            price=Decimal(100),
            closed_candle=None,
            observed_at_ms=60_001,
            position=PositionView(Decimal("0.5"), Decimal(100), 2),
        )
        _require(
            candidate.evaluate(later) == module.evaluate(later),
            "checkpoint recovery changed an in-progress protection decision",
        )

    for name, action in (
        ("tef_missing_features_warmup", warmup),
        ("tef_long_entry", long_entry),
        ("tef_close_before_reverse", reversal),
        ("tef_local_stop_during_pause", stop_while_paused),
        ("tef_stop_latch_manual_resume", no_reentry_after_stop),
        ("tef_recovered_close_wick_stop", wick_stop),
        ("tef_pre_entry_wick_ignored", before_entry_wick),
        ("tef_cancel_before_protective_replacement", protection_cancel),
        ("tef_partial_stop_never_reexpands", residual_stop),
        ("tef_unsubmitted_dust_never_latches", dust_stop),
        ("tef_checkpoint_keeps_stop_progress", checkpoint_stop),
    ):
        cases.append(_case(name, "family_specific", action))
    return cases


def validate_target_quantity(factory, instrument):
    cases = []

    def target_entry():
        decision = factory({"quantity": "2"}).evaluate(context(instrument))
        _require(
            len(decision.orders) == 1
            and decision.orders[0].quantity == 2
            and decision.orders[0].side == Side.BUY,
            "quantity target was not preserved",
        )

    def target_reverse():
        module = factory({"quantity": "-2"})
        decision = module.evaluate(
            context(instrument, position=PositionView(Decimal(1), Decimal(100), 1))
        )
        _require(
            len(decision.orders) == 1
            and decision.orders[0].reducing
            and decision.orders[0].quantity == 1,
            "quantity reversal must close before new direction",
        )

    def target_reduce_pause():
        module = factory({"quantity": "0.5"})
        decision = module.evaluate(
            context(
                instrument,
                position=PositionView(Decimal(1), Decimal(100), 1),
                entries_allowed=False,
            )
        )
        _require(
            len(decision.orders) == 1
            and decision.orders[0].reducing
            and decision.orders[0].quantity == Decimal("0.5"),
            "entry pause incorrectly blocks risk reduction",
        )

    for name, action in (
        ("quantity_target_entry", target_entry),
        ("quantity_target_close_before_reverse", target_reverse),
        ("quantity_target_reduce_while_paused", target_reduce_pause),
    ):
        cases.append(_case(name, "family_specific", action))
    return cases


def run_conformance(registry, identity: str, configuration: dict | None = None):
    instrument = Instrument("binance", Market.USDM, "BTCUSDT")

    def factory(config=None):
        return registry.create(identity, instrument, configuration if config is None else config)

    cases = executor_cases(instrument) + module_cases(factory, instrument)
    validator = registry.validator(identity)
    family_status = "not_provided"
    if validator is not None:
        try:
            extra = validator(factory, instrument)
            if (
                not isinstance(extra, list)
                or not extra
                or not all(
                    isinstance(item, dict)
                    and item.get("status") in {"passed", "failed", "skipped"}
                    and isinstance(item.get("id"), str)
                    for item in extra
                )
            ):
                raise ValidationError(
                    "family validator must return a nonempty list of case records"
                )
            json.dumps(extra, allow_nan=False)
            cases.extend({**item, "scope": "family_specific"} for item in extra)
            family_status = "provided"
        except Exception as exc:
            cases.append(
                {
                    "id": "family_validator",
                    "scope": "family_specific",
                    "status": "failed",
                    "error": type(exc).__name__,
                }
            )
    failed = sum(case["status"] == "failed" for case in cases)
    skipped = sum(case["status"] == "skipped" for case in cases)
    return {
        "schema_version": 1,
        "module_id": identity,
        "status": "failed"
        if failed
        else "passed"
        if family_status == "provided" and not skipped
        else "partial",
        "passed": sum(case["status"] == "passed" for case in cases),
        "failed": failed,
        "skipped": skipped,
        "family_validation": family_status,
        "cases": cases,
        "limitations": [
            "Offline smoke validation only; no exchange orders or profitability proof.",
            "Synthetic executor probes verify common boundaries separately "
            "from the selected family's behavior.",
            "Legacy numeric parity, live fills, crash recovery and full strategy "
            "semantics require the project test suite.",
        ],
    }
