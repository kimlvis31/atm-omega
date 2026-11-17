"""Optional external inputs preserve ordinary durable margin-session identities."""

import json
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256

import pytest
from test_margin_session import BTC, bars, configuration, new, run, tables

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.external_features import (
    ExternalFeatureBinding,
    ExternalFeatureDelivery,
    ExternalFeaturePin,
    ExternalFeatureSnapshot,
    semantic_candle_sha256,
)
from omega.domain.inference import digest
from omega.domain.models import Side
from omega.domain.trading import OrderProposal, TradeDecision
from omega.ports.modules import ModuleDescriptor
from omega.runtime import margin_session
from omega.runtime.margin_session import MarginTradingSession, prepare_margin_configuration


def test_no_external_features_preserve_accepted_session_bytes(tmp_path):
    # Frozen before optional NN integration; no empty opt-in keys or schema rewrite.
    session = new(tmp_path)
    try:
        assert (
            session.config_hash
            == "cff0099f642df5a79aeb483ab1eda017680bb7494426d8c0da4c70e6a4504612"
        )
        assert sha256(session.path.read_bytes()).hexdigest() == (
            "726aa861770e1dc4d259075ffe9e13c1dc89f1fc230886376b5e20ff9950663c"
        )
        session.resume("start")
        run(session, 0)
        assert sha256(session.path.read_bytes()).hexdigest() == (
            "54bd723a6654fa9f2216ec24a9aaf1f655782d7f91be14d3f9487ecb83bb131b"
        )
        assert session._meta["last_frame"]["digest"] == (
            "5e6e6100ceb63ca2f7e3153b3b56668b5c8d717d420f7d4b162da48d808112c9"
        )
        assert {
            key: adapter.snapshot().checkpoint.last_bar_digest
            for key, adapter in session._adapters.items()
        } == {
            "btc-ts": "fd83af205838f8c3e42d2c1af5d7d268bc6646c569216604c0ca3475bd266e12",
            "eth-ts": "2ba59cb594f44a50959a65824f6610fab977b7454384d218018192f4d17ef335",
        }
        session.pause("pause")
        run(session, 1)
        expected = "da92f577ef868e5fc1605c92c3a3edfe53ac3c3b048eb7d93d2a75329ada924f"
        assert sha256(session.path.read_bytes()).hexdigest() == expected
        assert session._meta["schema_version"] == 1
        assert "external_features" not in session._meta["last_frame"]
    finally:
        session.close()
    restored = new(tmp_path, create=False)
    try:
        assert sha256(restored.path.read_bytes()).hexdigest() == expected
    finally:
        restored.close()


class NNModule:
    """Deliberately requires NN for entries, while owning independent local protection."""

    def __init__(
        self, instrument, *, mixed=False, after_evaluate=None, side="long", field="NN.score"
    ):
        self.instrument, self.mixed, self.side = instrument, mixed, side
        self.after_evaluate = after_evaluate
        self.field = field
        self.last_open_time_ms = None
        self.descriptor = ModuleDescriptor(
            "nn_fixture",
            "1",
            frozenset({field} | ({"0_EMA_0_EMA"} if mixed else set())),
            True,
        )
        self.protection_calls = 0

    def checkpoint(self):
        return {"version": 1, "cursor": self.last_open_time_ms, "protected": self.protection_calls}

    def restore(self, document):
        assert set(document) == {"version", "cursor", "protected"}
        self.last_open_time_ms = document["cursor"]
        self.protection_calls = document["protected"]

    def resume(self):
        pass

    def evaluate_protection(self, context):
        self.last_open_time_ms = context.closed_candle.open_time_ms
        self.protection_calls += 1
        quantity = context.position.quantity
        touched = (
            quantity > 0
            and context.closed_candle.low <= D(95)
            or quantity < 0
            and context.closed_candle.high >= D(105)
        )
        if quantity and touched:
            return TradeDecision(
                (
                    OrderProposal(
                        Side.SELL if quantity > 0 else Side.BUY, abs(quantity), reducing=True
                    ),
                ),
                "local_protection",
            )
        return TradeDecision(reason="protect_without_external_input")

    def evaluate(self, context):
        self.last_open_time_ms = context.closed_candle.open_time_ms
        score = context.features[self.field]
        if self.mixed:
            assert "0_EMA_0_EMA" in context.features
        if self.after_evaluate is not None:
            self.after_evaluate()
        if score > 0 and not context.position.quantity and context.entries_allowed:
            return TradeDecision(
                (OrderProposal(Side.BUY if self.side == "long" else Side.SELL, D(1)),), "nn_entry"
            )
        return self.evaluate_protection(context)


class NNRegistry:
    def __init__(self, *, mixed=False, after_evaluate=None, side="long", field="NN.score"):
        self.mixed, self.after_evaluate, self.side = mixed, after_evaluate, side
        self.field = field

    def create(self, module, instrument, configuration):
        assert module == "nn_fixture"
        return NNModule(
            instrument,
            mixed=self.mixed,
            after_evaluate=self.after_evaluate,
            side=self.side,
            field=self.field,
        )


class Provider:
    """Fake cached authority/pin port; the session and all finance use real files."""

    def __init__(self, root):
        self.root = root
        self.binding = ExternalFeatureBinding(
            "causal_nn", "deployment1", "stream1", BTC, "a" * 64, "b" * 64, ("NN.score",)
        )
        self.clock = 60010
        self.available = True
        self.no_signal = False
        self.core_epoch = "core1"
        self.pins, self.released = {}, {}
        self.lookups = 0
        self.fail_pin = False
        self.pin_hook = None
        self.guard_allowed, self.guard_raise = True, False
        self.guard_calls, self.guard_depth = 0, 0

    def describe(self, provider_id, instance_id, stream, instrument):
        return self.binding

    def lookup(self, binding, candle, *, decision_at_ms):
        self.lookups += 1
        snapshot = None
        if self.available:
            snapshot = ExternalFeatureSnapshot(
                binding,
                self.core_epoch,
                "deployment1",
                "epoch1",
                digest({"request": candle.open_time_ms}),
                "c" * 64,
                "d" * 64,
                "e" * 64,
                "f" * 64,
                semantic_candle_sha256(candle),
                candle.open_time_ms,
                candle.close_time_ms,
                candle.close_time_ms + 1,
                candle.close_time_ms + 60000,
                "no_signal" if self.no_signal else "ready",
                () if self.no_signal else ((binding.fields[0], D(".75")),),
            )
        return ExternalFeatureDelivery(
            binding,
            ("no_signal" if self.no_signal else "ready") if self.available else "pending",
            snapshot,
            self.core_epoch,
            "deployment1",
            "epoch1",
            semantic_candle_sha256(candle),
            candle.open_time_ms,
            decision_at_ms,
            self.clock,
            "fixture_ready" if self.available else "fixture_pending",
        )

    def pin(self, consumer_id, frame_ref, snapshot, candle, *, decision_at_ms):
        saved = json.loads(next((self.root / "state/margin-sessions").glob("*.json")).read_text())[
            "data"
        ]["pending"]
        assert saved is not None and saved["digest"] == frame_ref
        pin = ExternalFeaturePin(
            self.binding.provider_id,
            consumer_id,
            frame_ref,
            self.binding.binding_sha256,
            snapshot.snapshot_sha256,
            self.clock,
        )
        self.pins.setdefault(pin.pin_id, pin)
        if self.pin_hook is not None:
            self.pin_hook()
        if self.fail_pin:
            raise StorageError("fake authority pin ACK loss")
        return self.pins[pin.pin_id]

    def authorize(self, pin, snapshot, candle, *, decision_at_ms):
        return (
            pin.pin_id in self.pins
            and pin.pin_id not in self.released
            and snapshot.core_epoch == self.core_epoch
            and snapshot.accepted_at_ms <= self.clock < snapshot.valid_until_ms
        )

    def release(self, pin, *, consumer_receipt_sha256):
        self.released[pin.pin_id] = consumer_receipt_sha256

    @contextmanager
    def entry_lease(self, pin, snapshot, candle, *, decision_at_ms):
        self.guard_calls += 1
        if self.guard_raise:
            raise StorageError("fake lease acquisition failed")
        self.guard_depth += 1
        try:
            yield self.guard_allowed and self.authorize(
                pin, snapshot, candle, decision_at_ms=decision_at_ms
            )
        finally:
            self.guard_depth -= 1


def external_configuration(provider, *, mixed=False):
    account, strategies = configuration()
    account["positions"] = account["positions"][:1]
    strategies = strategies[:1]
    strategies[0].update(
        module="nn_fixture",
        configuration={},
        analysis={"ema_period": 2} if mixed else {},
        external_features=[provider.binding.document()],
    )
    return account, strategies


def external_session(root, provider, *, create=True, registry=None, saved_configuration=None):
    registry = registry or NNRegistry()
    account, strategies = saved_configuration or external_configuration(
        provider, mixed=registry.mixed
    )
    return MarginTradingSession(
        root,
        account,
        strategies,
        0,
        create=create,
        registry=registry,
        external_feature_provider=provider,
        clock_ms=lambda: provider.clock,
    )


def external_run(session, provider, index, price="100", *, low=None):
    provider.clock = (index + 1) * 60000 + 10
    return session.run_once(
        (bars(index, price, low=low)[0],), as_of_ms=provider.clock, brackets=(tables()[0],)
    )


def test_typed_proof_graph_ownership_pinned_before_entry_and_restart_anchors(tmp_path):
    provider = Provider(tmp_path)
    registry = NNRegistry(mixed=True)
    session = external_session(tmp_path, provider, registry=registry)
    try:
        session.resume("start")
        external_run(session, provider, 0)
        # EMA has one-minute warmup; its graph outputs never acquire NN fields.
        assert "NN.score" not in session._meta["last_frame"]["features"]["btc-ts"]
        assert len(provider.pins) == 1
        external_run(session, provider, 1)
        assert session._meta["schema_version"] == 2
        frame = session._meta["last_frame"]
        assert frame["external_features"]["btc-ts"][0]["pin"]["frame_ref"] == frame["digest"]
        assert provider.released
        assert session.snapshot()["new_risk_ready"]
        assert len(session.execution.snapshot().pending) == 1
        saved = session.path.read_bytes()
    finally:
        session.close()
    before = provider.lookups
    restored = external_session(tmp_path, provider, create=False, registry=registry)
    try:
        assert provider.lookups == before
        assert restored.path.read_bytes() == saved
        external_run(restored, provider, 2)
        assert len(restored.execution.snapshot().account.ledger) == 1
    finally:
        restored.close()


@pytest.mark.parametrize(
    "change", ["missing", "wrong_fields", "wrong_instrument", "unregistered", "duplicate"]
)
def test_invalid_external_configuration_never_creates_financial_namespace(tmp_path, change):
    provider = Provider(tmp_path)
    account, strategies = external_configuration(provider)
    if change == "missing":
        del strategies[0]["external_features"]
    elif change == "wrong_fields":
        value = provider.binding
        strategies[0]["external_features"] = [
            ExternalFeatureBinding(
                value.provider_id,
                value.instance_id,
                value.stream,
                value.instrument,
                value.producer_sha256,
                value.contract_sha256,
                ("NN.other",),
            ).document()
        ]
    elif change == "wrong_instrument":
        strategies[0]["instrument"] = "binance:usdm:ETHUSDT"
    elif change == "unregistered":
        provider.binding = None
    else:
        strategies[0]["external_features"] *= 2
    with pytest.raises(ValidationError):
        prepare_margin_configuration(
            account, strategies, 0, registry=NNRegistry(), external_feature_provider=provider
        )
    assert not (tmp_path / "state").exists()


def test_missing_then_late_feature_never_re_evaluates_consumed_minute(tmp_path):
    provider = Provider(tmp_path)
    provider.available = False
    session = external_session(tmp_path, provider)
    try:
        session.resume("start")
        external_run(session, provider, 0)
        assert not session.execution.snapshot().pending
        first = deepcopy(session._meta["last_frame"])
        provider.available = True
        before = provider.lookups
        external_run(session, provider, 0)
        assert provider.lookups == before and session._meta["last_frame"] == first
        external_run(session, provider, 1)
        assert len(session.execution.snapshot().pending) == 1
    finally:
        session.close()


def test_expiry_between_staged_ts_and_dispatch_records_rejection_without_entry(tmp_path):
    provider = Provider(tmp_path)
    registry = NNRegistry(after_evaluate=lambda: setattr(provider, "clock", 120000))
    session = external_session(tmp_path, provider, registry=registry)
    try:
        session.resume("start")
        external_run(session, provider, 0)
        cp = session._adapters["btc-ts"].snapshot().checkpoint
        assert not session.execution.snapshot().pending
        assert len(cp.rejected_intents) == 1
        assert cp.rejected_intents[0].reason == "external_input_unavailable"
        assert not session.snapshot()["new_risk_ready"]
    finally:
        session.close()


@pytest.mark.parametrize("mode", ["cross", "isolated"])
@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("failure", ["missing", "pin_ack_loss", "epoch"])
def test_external_failure_keeps_original_fill_and_local_position_protection(
    tmp_path, failure, side, mode
):
    provider = Provider(tmp_path)
    account, strategies = external_configuration(provider)
    account["positions"][0]["margin_mode"] = mode
    if mode == "isolated":
        account["positions"][0]["isolated_collateral"] = "100"
    session = external_session(
        tmp_path,
        provider,
        registry=NNRegistry(side=side),
        saved_configuration=(account, strategies),
    )
    try:
        session.resume("start")
        external_run(session, provider, 0)
        original = session.execution.snapshot().pending[0]
        if failure == "missing":
            provider.available = False
        elif failure == "pin_ack_loss":
            provider.fail_pin = True
        else:
            provider.pin_hook = lambda: setattr(provider, "core_epoch", "core2")
        # The 60010ms decision first becomes eligible at open=120000ms.
        external_run(session, provider, 1)
        assert not session.execution.snapshot().account.ledger
        external_run(session, provider, 2)
        account = session.execution.snapshot().account
        assert len(account.ledger) == 1 and account.ledger[0].fact.order_id == original.order_id
        stop = "90" if side == "long" else "110"
        external_run(session, provider, 3, stop)
        pending = session.execution.snapshot().pending
        assert len(pending) == 1 and pending[0].original.proposal.reducing
        external_run(session, provider, 4, stop)
        external_run(session, provider, 5, stop)
        assert len(session.execution.snapshot().account.ledger) == 2
        assert session.execution.snapshot().account.positions[0].quantity == 0
    finally:
        session.close()


@pytest.mark.parametrize(
    "boundary", ["frozen_before_pin", "pin_ack_before_metadata", "pin_metadata_ack"]
)
def test_restart_frozen_proof_never_looks_up_replacement_and_pin_retry_is_idempotent(
    tmp_path, monkeypatch, boundary
):
    provider = Provider(tmp_path)
    session = external_session(tmp_path, provider)
    session.resume("start")
    original = margin_session.atomic_json
    fired = []

    def lost_ack(path, envelope):
        frame = envelope.get("data", {}).get("pending")
        entry = frame["external_features"]["btc-ts"][0] if frame else None
        wanted = entry is not None and (
            (boundary == "frozen_before_pin" and entry["pin"] is None)
            or (boundary != "frozen_before_pin" and entry["pin"] is not None)
        )
        if path == session.path and wanted and not fired:
            fired.append(True)
            if boundary != "pin_ack_before_metadata":
                original(path, envelope)
            raise OSError("synthetic consumer frame ACK loss")
        original(path, envelope)

    monkeypatch.setattr(margin_session, "atomic_json", lost_ack)
    try:
        with pytest.raises(StorageError):
            external_run(session, provider, 0)
        assert fired
    finally:
        session.close()
    monkeypatch.setattr(margin_session, "atomic_json", original)
    calls = provider.lookups
    restored = external_session(tmp_path, provider, create=False)
    try:
        frozen = deepcopy(restored._meta["pending"])
        assert frozen is not None
        restored.run_once((), as_of_ms=60011, entries_allowed=False)
        assert provider.lookups == calls
        assert restored._meta["last_frame"]["digest"] == frozen["digest"]
        assert (
            restored._meta["last_frame"]["external_features"]["btc-ts"][0]["delivery"]
            == frozen["external_features"]["btc-ts"][0]["delivery"]
        )
        assert len(provider.pins) == 1
        assert not restored.execution.snapshot().pending
    finally:
        restored.close()


def test_registered_generic_namespace_is_not_assumed_to_be_nn(tmp_path):
    provider = Provider(tmp_path)
    provider.binding = replace(provider.binding, fields=("Other.score",))
    session = external_session(tmp_path, provider, registry=NNRegistry(field="Other.score"))
    try:
        session.resume("start")
        external_run(session, provider, 0)
        assert len(session.execution.snapshot().pending) == 1
    finally:
        session.close()


@pytest.mark.parametrize("fault", ["false", "enter_error"])
def test_current_entry_lease_failure_rejects_only_unsent_entry(tmp_path, fault):
    provider = Provider(tmp_path)
    provider.guard_allowed = fault != "false"
    provider.guard_raise = fault == "enter_error"
    session = external_session(tmp_path, provider)
    try:
        session.resume("start")
        external_run(session, provider, 0)
        assert provider.guard_calls == 1 and provider.guard_depth == 0
        assert not session.execution.snapshot().pending
        cp = session._adapters["btc-ts"].snapshot().checkpoint
        assert cp.rejected_intents[0].reason == "external_input_unavailable"
    finally:
        session.close()


def test_entry_lease_spans_actual_dispatch_and_body_error_is_not_swallowed(tmp_path, monkeypatch):
    provider = Provider(tmp_path)
    session = external_session(tmp_path, provider)
    original = session.execution.submit_intent

    def failed_submit(*args, **kwargs):
        assert provider.guard_depth == 1
        raise StorageError("actual financial dispatch failure")

    monkeypatch.setattr(session.execution, "submit_intent", failed_submit)
    try:
        session.resume("start")
        with pytest.raises(StorageError, match="actual financial dispatch"):
            external_run(session, provider, 0)
        assert provider.guard_calls == 1 and provider.guard_depth == 0
        assert not session.execution.snapshot().pending
        assert session._adapters["btc-ts"].snapshot().checkpoint.pending is not None
        monkeypatch.setattr(session.execution, "submit_intent", original)
        provider.guard_allowed = False
        session.reconcile()
        assert not session.execution.snapshot().pending
    finally:
        session.close()


def test_staged_checkpoint_ack_loss_restart_uses_current_entry_lease(tmp_path, monkeypatch):
    provider = Provider(tmp_path)
    session = external_session(tmp_path, provider)
    session.resume("start")
    store = session._adapters["btc-ts"].store
    original, fired = store.save, []

    def lost_stage_ack(state, **kwargs):
        original(state, **kwargs)
        if state.pending is not None and not fired:
            fired.append(True)
            raise StorageError("actual stage checkpoint ACK lost")

    monkeypatch.setattr(store, "save", lost_stage_ack)
    try:
        with pytest.raises(StorageError, match="stage checkpoint"):
            external_run(session, provider, 0)
        assert provider.guard_calls == 0 and not session.execution.snapshot().pending
    finally:
        session.close()
    provider.guard_allowed = False
    restored = external_session(tmp_path, provider, create=False)
    try:
        restored.run_once((), as_of_ms=60011)
        assert provider.guard_calls == 1 and provider.guard_depth == 0
        assert not restored.execution.snapshot().pending
        assert (
            restored._adapters["btc-ts"].snapshot().checkpoint.rejected_intents[0].reason
            == "external_input_unavailable"
        )
    finally:
        restored.close()


def test_no_signal_consumes_protection_only_without_invented_numeric_value(tmp_path):
    provider = Provider(tmp_path)
    provider.no_signal = True
    session = external_session(tmp_path, provider)
    try:
        session.resume("start")
        external_run(session, provider, 0)
        frame = session._meta["last_frame"]
        assert frame["external_features"]["btc-ts"][0]["delivery"]["disposition"] == "no_signal"
        assert session._external.decision_features(frame, "btc-ts") == {}
        assert not session.execution.snapshot().pending
        assert not provider.pins
        provider.no_signal = False
        external_run(session, provider, 1)
        assert len(session.execution.snapshot().pending) == 1
    finally:
        session.close()


def test_external_module_without_independent_protection_is_rejected(tmp_path):
    provider = Provider(tmp_path)
    account, strategies = external_configuration(provider)

    class Unsafe(NNRegistry):
        def create(self, *args):
            module = super().create(*args)
            module.evaluate_protection = None
            return module

    with pytest.raises(ValidationError, match="independent protection"):
        prepare_margin_configuration(
            account, strategies, 0, registry=Unsafe(), external_feature_provider=provider
        )
    assert not (tmp_path / "state").exists()


def test_restart_without_current_provider_keeps_financial_fill_and_fsl(tmp_path):
    provider = Provider(tmp_path)
    selected = external_configuration(provider)
    session = external_session(tmp_path, provider, saved_configuration=selected)
    session.resume("start")
    external_run(session, provider, 0)
    session.close()
    calls = provider.lookups
    restored = MarginTradingSession(
        tmp_path, *selected, 0, create=False, registry=NNRegistry(), clock_ms=lambda: provider.clock
    )
    try:
        assert not restored.snapshot()["new_risk_ready"]
        for index, price in ((1, "100"), (2, "100"), (3, "90"), (4, "90"), (5, "90")):
            external_run(restored, provider, index, price)
        assert provider.lookups == calls
        assert len(restored.execution.snapshot().account.ledger) == 2
        assert restored.execution.snapshot().account.positions[0].quantity == 0
    finally:
        restored.close()


@pytest.mark.parametrize("change", ["pin_consumer", "candle", "value", "unknown_field"])
def test_pure_frozen_validation_rejects_changed_proof_without_current_lookup(tmp_path, change):
    provider = Provider(tmp_path)
    session = external_session(tmp_path, provider)
    try:
        session.resume("start")
        external_run(session, provider, 0)
        frame = deepcopy(session._meta["last_frame"])
        entry = frame["external_features"]["btc-ts"][0]
        if change == "pin_consumer":
            entry["pin"] = replace(
                ExternalFeaturePin.parse(entry["pin"]), consumer_id="other_consumer"
            ).document()
        elif change in {"candle", "value"}:
            delivery = ExternalFeatureDelivery.parse(entry["delivery"])
            snapshot = replace(
                delivery.snapshot,
                **(
                    {"candle_sha256": "9" * 64}
                    if change == "candle"
                    else {"values": (("NN.score", D(".50")),)}
                ),
            )
            entry["delivery"] = replace(
                delivery, snapshot=snapshot, candle_sha256=snapshot.candle_sha256
            ).document()
            frame["digest"] = session._frame_digest(frame)
        else:
            entry["extra"] = True
        before = provider.lookups
        with pytest.raises((ValidationError, ProtocolError)):
            session._validate_frame(frame, 0)
        assert provider.lookups == before
        assert len(session.execution.snapshot().pending) == 1
    finally:
        session.close()
