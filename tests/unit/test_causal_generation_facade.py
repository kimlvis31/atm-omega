"""Original-provider fakes exercise facade fencing without any store/model IO."""

import threading
from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal

import pytest

from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.domain.external_features import (
    ExternalFeatureBinding,
    ExternalFeatureDelivery,
    ExternalFeaturePin,
    ExternalFeatureSnapshot,
    semantic_candle_sha256,
)
from omega.domain.inference import decoded, digest, encoded
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.causal_generation_facade import GenerationBinding, GenerationFeatureProvider
from omega.runtime.causal_recovery_records import (
    FeatureMutationRejected,
    FeatureMutationUnknown,
    MutationJournalAck,
    MutationRecord,
)

SHA = "a" * 64
FRAME = "b" * 64
RECEIPT = "c" * 64


class Broker:
    def __init__(self):
        self.records = {}
        self.arguments = {}
        self.calls = []
        self.prepare_error = None
        self.finish_error = None
        self.on_prepare = None
        self.on_finish = None
        self.on_consume = None
        self.consume_error = None
        self.issued = {}
        self.consumed = {}
        self.consume_calls = []

    def ack(self, ref, state, *, result_json=None, code=None):
        ack = MutationJournalAck(ref.sha256, state, 1, SHA, SHA, True, result_json, code)
        if state == "terminal":
            self.issued[ref.sha256] = ack
        return ack

    def prepare_mutation(self, ref, args_json):
        self.calls.append(("prepare", ref, args_json))
        self.arguments[ref.sha256] = args_json
        if self.on_prepare:
            self.on_prepare(ref)
        if self.prepare_error:
            raise self.prepare_error
        if ref.sha256 not in self.records:
            self.records[ref.sha256] = self.ack(ref, "prepared")
        return self.records[ref.sha256]

    def finish_mutation(self, ref, result_json):
        self.calls.append(("finish", ref, result_json))
        if self.on_finish:
            self.on_finish(ref)
        if self.finish_error:
            raise self.finish_error
        self.records[ref.sha256] = self.ack(ref, "terminal", result_json=result_json)
        return self.records[ref.sha256]

    def mark_unknown(self, ref, code):
        self.calls.append(("unknown", ref, code))
        previous = self.records.get(ref.sha256)
        if previous and previous.state == "terminal":
            return previous
        self.records[ref.sha256] = self.ack(ref, "unknown", code=code)
        return self.records[ref.sha256]

    def lookup_mutation(self, ref):
        self.calls.append(("lookup", ref, None))
        ack = self.records.get(ref.sha256)
        if ack is None:
            return None
        return MutationRecord(ref, self.arguments[ref.sha256], ack)

    def consume_terminal(self, ref, ack, witness):
        self.consume_calls.append((ref, ack, witness))
        if self.on_consume:
            self.on_consume(ref, ack, witness)
        if self.consume_error:
            raise self.consume_error
        previous = self.consumed.get(ref.sha256)
        if previous is not None and previous[0] is ack and previous[1] is witness:
            return True
        if self.issued.get(ref.sha256) is not ack or witness.ack is not ack or witness.ref != ref:
            return False
        self.consumed[ref.sha256] = (ack, witness)
        return True


class Provider:
    def __init__(self):
        instrument = Instrument("binance", Market.USDM, "BTCUSDT")
        self.binding = ExternalFeatureBinding(
            "nn", "instance", "stream", instrument, SHA, SHA, ("NN.score",)
        )
        self.candle = Candle(instrument, 0, 60000, *(Decimal(v) for v in (100, 102, 99, 101, 2)))
        self.snapshot = ExternalFeatureSnapshot(
            self.binding,
            "core",
            "deployment",
            "epoch",
            SHA,
            SHA,
            SHA,
            SHA,
            SHA,
            semantic_candle_sha256(self.candle),
            0,
            60000,
            60000,
            120000,
            "ready",
            (("NN.score", Decimal("0.5")),),
        )
        self.calls = []
        self.members = {}
        self.entry_open = False
        self.on_dispatch = None
        self.dispatch_error = None
        self.metadata_error = False
        self.risk_allowed = True

    def list_bindings(self):
        if self.metadata_error:
            raise AssertionError("cached facade views called provider")
        return (self.binding,)

    def lookup(self, binding, candle, *, decision_at_ms):
        self.calls.append("lookup")
        return ExternalFeatureDelivery(
            binding,
            "ready",
            self.snapshot,
            "core",
            "deployment",
            "epoch",
            semantic_candle_sha256(candle),
            candle.open_time_ms,
            decision_at_ms,
            decision_at_ms,
            "ready",
        )

    def offer_committed(self, binding, candle):
        return self.lookup(binding, candle, decision_at_ms=60000)

    def authorize(self, pin, snapshot, candle, *, decision_at_ms):
        self.calls.append("authorize")
        return self.risk_allowed and self.contains_pin(pin)

    def contains_pin(self, pin):
        return self.members.get(pin.pin_id) == pin

    @contextmanager
    def entry_lease(self, pin, snapshot, candle, *, decision_at_ms):
        self.calls.append("entry")
        self.entry_open = True
        try:
            yield self.authorize(pin, snapshot, candle, decision_at_ms=decision_at_ms)
        finally:
            self.entry_open = False

    def dispatch_feature_mutation(self, ref, args_json):
        self.calls.append((ref, args_json))
        if self.on_dispatch:
            self.on_dispatch(ref)
        if self.dispatch_error:
            raise self.dispatch_error
        args = decoded(args_json)
        if ref.kind == "pin":
            snapshot = ExternalFeatureSnapshot.parse(args["snapshot"])
            pin = ExternalFeaturePin(
                snapshot.binding.provider_id,
                args["consumer_id"],
                args["frame_ref"],
                snapshot.binding.binding_sha256,
                snapshot.snapshot_sha256,
                60000,
            )
            self.members[pin.pin_id] = pin
            return pin
        pin = ExternalFeaturePin.parse(args["pin"])
        self.members.pop(pin.pin_id, None)
        return None


def setup():
    broker, provider = Broker(), Provider()
    facade = GenerationFeatureProvider(broker, lambda: 60001)
    binding = GenerationBinding("core", 0, "actor0", provider)
    facade.bind_initial(binding)
    return facade, broker, provider, binding


def pin(facade, provider, *, frame=FRAME):
    return facade.pin("consumer", frame, provider.snapshot, provider.candle, decision_at_ms=60000)


def candidate():
    return GenerationBinding("core", 1, "actor1", Provider())


def test_cached_views_and_held_deliveries_do_not_call_original_provider():
    facade, _, provider, _ = setup()
    provider.metadata_error = True
    assert facade.list_bindings() == (provider.binding,)
    assert (
        facade.describe("nn", "instance", "stream", provider.candle.instrument) == provider.binding
    )
    facade.hold(SHA)
    assert facade.list_bindings() == ()
    assert facade.describe("nn", "instance", "stream", provider.candle.instrument) is None
    delivery = facade.lookup(provider.binding, provider.candle, decision_at_ms=60000)
    assert delivery.disposition == "unavailable" and delivery.observed_at_ms == 60001
    assert facade.offer_committed(provider.binding, provider.candle).snapshot is None
    assert provider.calls == []


def test_mutation_prepare_dispatch_terminal_order_and_immutable_reference():
    facade, broker, provider, _ = setup()
    observed = []
    broker.on_prepare = lambda ref: observed.append(("prepared", ref))
    provider.on_dispatch = lambda ref: observed.append(("dispatch", ref))
    broker.on_finish = lambda ref: observed.append(("terminal", ref))
    result = pin(facade, provider)
    assert [name for name, _ in observed] == ["prepared", "dispatch", "terminal"]
    assert all(ref == observed[0][1] for _, ref in observed)
    ref = observed[0][1]
    assert ref.pin_id == result.pin_id
    assert ref.arguments_sha256 == digest(decoded(broker.calls[0][2]))
    assert facade.status().hot_mutations == 0
    assert facade.authorize(result, provider.snapshot, provider.candle, decision_at_ms=60000)
    assert pin(facade, provider) == result
    assert len([row for row in provider.calls if type(row) is tuple]) == 1


def test_exact_pin_membership_cannot_be_created_by_parsing_or_changed_pin_bytes():
    facade, _, provider, _ = setup()
    forged = ExternalFeaturePin(
        "nn",
        "consumer",
        FRAME,
        provider.binding.binding_sha256,
        provider.snapshot.snapshot_sha256,
        60000,
    )
    assert not facade.authorize(forged, provider.snapshot, provider.candle, decision_at_ms=60000)
    real = pin(facade, provider)
    changed = replace(real, created_at_ms=60001)
    assert changed.pin_id == real.pin_id
    assert not facade.authorize(changed, provider.snapshot, provider.candle, decision_at_ms=60000)
    with pytest.raises(TradingBlocked):
        facade.release(changed, consumer_receipt_sha256=RECEIPT)
    restored = ExternalFeaturePin.parse(real.document())
    assert facade.authorize(restored, provider.snapshot, provider.candle, decision_at_ms=60000)


def test_hold_allows_original_release_and_duplicate_receipt_but_conflict_rejected():
    facade, _, provider, _ = setup()
    original = pin(facade, provider)
    fence = facade.hold(SHA)
    assert not facade.authorize(original, provider.snapshot, provider.candle, decision_at_ms=60000)
    with pytest.raises(TradingBlocked):
        pin(facade, provider, frame=SHA)
    facade.release(original, consumer_receipt_sha256=RECEIPT)
    facade.release(original, consumer_receipt_sha256=RECEIPT)
    with pytest.raises(ValidationError):
        facade.release(original, consumer_receipt_sha256=SHA)
    assert facade.close_settlement(fence)
    assert facade.install_cas(fence, candidate())
    # Historical exact consumed receipt is cached evidence, never new authority.
    facade.release(original, consumer_receipt_sha256=RECEIPT)
    assert not facade.authorize(original, provider.snapshot, provider.candle, decision_at_ms=60000)


def test_settlement_close_blocks_remaining_pins_and_install_never_drops_them():
    facade, _, provider, _ = setup()
    original = pin(facade, provider)
    fence = facade.hold(SHA)
    assert facade.close_settlement(fence)
    with pytest.raises(TradingBlocked):
        facade.release(original, consumer_receipt_sha256=RECEIPT)
    assert not facade.install_cas(fence, candidate())
    assert facade.status().pins == 1


def test_entry_lease_returns_original_provider_context_before_facade_lock(monkeypatch):
    facade, _, provider, _ = setup()
    original = pin(facade, provider)
    return_borrow = facade._return_borrow

    def check_return(*, entry=False):
        assert not provider.entry_open
        return_borrow(entry=entry)

    monkeypatch.setattr(facade, "_return_borrow", check_return)
    with facade.entry_lease(
        original, provider.snapshot, provider.candle, decision_at_ms=60000
    ) as ok:
        assert ok and provider.entry_open
        assert facade.status().entry_leases == 1
        fence = facade.hold(SHA)
        facade.close_settlement(fence)
        assert not facade.drain_event.is_set()
        assert not facade.install_cas(fence, candidate())
        # An already admitted financial intent keeps its original entry lease.
        assert ok
    assert facade.drain_event.is_set() and facade.status().entry_leases == 0
    # A fresh entry cannot cross the hold, even with the exact previous pin.
    with facade.entry_lease(
        original, provider.snapshot, provider.candle, decision_at_ms=60000
    ) as ok:
        assert not ok


def test_entry_callback_can_take_facade_lock_without_inversion(monkeypatch):
    facade, _, provider, _ = setup()
    original = pin(facade, provider)
    entry = provider.entry_lease

    @contextmanager
    def checked(*args, **kwargs):
        assert facade._lock.acquire(blocking=False)
        facade._lock.release()
        with entry(*args, **kwargs) as result:
            yield result
        assert facade._lock.acquire(blocking=False)
        facade._lock.release()

    monkeypatch.setattr(provider, "entry_lease", checked)
    with facade.entry_lease(
        original, provider.snapshot, provider.candle, decision_at_ms=60000
    ) as ok:
        assert ok


@pytest.mark.parametrize("phase", ["prepare", "terminal"])
def test_broker_rpc_timeout_is_not_underlying_recording_failure(phase):
    facade, broker, provider, _ = setup()
    if phase == "prepare":
        broker.prepare_error = TimeoutError()
    else:
        broker.finish_error = TimeoutError()
    with pytest.raises(FeatureMutationUnknown) as caught:
        pin(facade, provider)
    assert caught.value.phase == phase and not caught.value.recording_unknown
    assert facade.status().hot_mutations == 1 and facade.status().recording_unknown == 0
    if phase == "prepare":
        assert provider.calls == []
        ack = broker.ack(
            caught.value.ref, "terminal", result_json=encoded({"not_dispatched": True})
        )
        assert facade.settle_not_dispatched(caught.value.ref, ack)
    else:
        _, ref, result_json = next(row for row in broker.calls if row[0] == "finish")
        assert facade.reconcile_mutation(ref, broker.ack(ref, "terminal", result_json=result_json))
        assert facade.status().pins == 1
    assert facade.status().hot_mutations == 0


def test_prepare_actual_recording_unknown_is_sticky_and_never_dispatches():
    facade, broker, provider, _ = setup()
    broker.prepare_error = StorageError("unknown IO")
    with pytest.raises(FeatureMutationUnknown) as caught:
        pin(facade, provider)
    assert caught.value.recording_unknown and provider.calls == []
    assert facade.status().recording_unknown == 1
    ack = broker.ack(caught.value.ref, "terminal", result_json=encoded({"not_dispatched": True}))
    assert not facade.settle_not_dispatched(caught.value.ref, ack)
    fence = facade.hold(SHA)
    facade.close_settlement(fence)
    assert not facade.install_cas(fence, candidate())


def test_positive_predispatch_capacity_refusal_releases_local_slot():
    facade, broker, provider, _ = setup()
    broker.prepare_error = FeatureMutationRejected()
    with pytest.raises(FeatureMutationRejected):
        pin(facade, provider)
    assert provider.calls == []
    assert facade.status().hot_mutations == 0 and facade.status().borrows == 0


def test_dispatch_timeout_retains_same_ref_and_late_ack_only_updates_original_generation():
    facade, broker, provider, _ = setup()
    provider.dispatch_error = TimeoutError()
    with pytest.raises(FeatureMutationUnknown) as caught:
        pin(facade, provider)
    ref = caught.value.ref
    with pytest.raises(FeatureMutationUnknown) as retry:
        pin(facade, provider)
    assert retry.value.ref == ref
    assert len(provider.calls) == 1
    fence = facade.hold(SHA)
    facade.close_settlement(fence)
    assert not facade.install_cas(fence, candidate())
    result = ExternalFeaturePin(
        "nn",
        "consumer",
        FRAME,
        provider.binding.binding_sha256,
        provider.snapshot.snapshot_sha256,
        60000,
    )
    args_json = broker.calls[0][2]
    result_json = facade.result_json(ref, args_json, result)
    bad = broker.ack(replace(ref, mutation_token="foreign"), "terminal", result_json=result_json)
    with pytest.raises(StorageError):
        facade.reconcile_mutation(ref, bad)
    provider.members[result.pin_id] = result  # Original owner completed after RPC timeout.
    assert facade.reconcile_mutation(ref, broker.ack(ref, "terminal", result_json=result_json))
    assert facade.status().pins == 1 and facade.status().held
    assert not facade.install_cas(fence, candidate())


def test_late_terminal_callback_cannot_reorder_or_change_observed_result():
    facade, broker, provider, _ = setup()
    broker.finish_error = TimeoutError()
    with pytest.raises(FeatureMutationUnknown) as caught:
        pin(facade, provider)
    _, ref, result_json = next(row for row in broker.calls if row[0] == "finish")
    document = decoded(result_json)
    changed = ExternalFeaturePin.parse(document["pin"])
    document["pin"] = replace(changed, created_at_ms=60001).document()
    with pytest.raises(ValidationError):
        facade.reconcile_mutation(ref, broker.ack(ref, "terminal", result_json=encoded(document)))
    assert facade.reconcile_mutation(
        caught.value.ref, broker.ack(ref, "terminal", result_json=result_json)
    )


def test_prepare_hold_close_race_retains_admitted_original_borrow():
    facade, broker, provider, _ = setup()
    entered, proceed = threading.Event(), threading.Event()
    outcome = []

    def prepared(ref):
        entered.set()
        assert proceed.wait(2)

    broker.on_prepare = prepared
    thread = threading.Thread(target=lambda: outcome.append(pin(facade, provider)))
    thread.start()
    try:
        assert entered.wait(2)
        fence = facade.hold(SHA)
        assert facade.status().borrows == 1
        facade.close_settlement(fence)
        assert not facade.install_cas(fence, candidate())
        proceed.set()
        thread.join(2)
        assert not thread.is_alive() and len(outcome) == 1
        assert facade.status().held and facade.status().pins == 1
    finally:
        proceed.set()
        thread.join(2)


def test_install_pointer_cas_shutdown_and_manual_readiness_are_separate():
    facade, _, _, _ = setup()
    first = facade.hold(SHA)
    second = facade.hold(FRAME)
    assert not facade.close_settlement(first)
    assert facade.close_settlement(second)
    replacement = candidate()
    assert facade.install_cas(second, replacement)
    status = facade.status()
    assert status.generation == 1 and status.held
    assert facade.list_bindings() == ()
    assert not facade.enable_current(1, "actor0", expected_control_revision=status.control_revision)
    assert facade.enable_current(1, "actor1", expected_control_revision=status.control_revision)
    assert facade.list_bindings() == (replacement.provider.binding,)
    fence = facade.hold(RECEIPT)
    facade.close_settlement(fence)
    facade.fence_shutdown()
    assert not facade.install_cas(fence, GenerationBinding("core", 2, "actor2", Provider()))
    assert not facade.enable_current(
        1, "actor1", expected_control_revision=facade.status().control_revision
    )


def test_original_closed_flag_never_grants_retirement_or_install():
    facade, _, provider, _ = setup()
    provider.closed = True
    pin(facade, provider)
    fence = facade.hold(SHA)
    facade.close_settlement(fence)
    assert not facade.install_cas(fence, candidate())
    assert facade.status().pins == 1


def test_consumed_terminal_pin_replay_does_not_resurrect_membership():
    facade, _, provider, _ = setup()
    original = pin(facade, provider)
    facade.release(original, consumer_receipt_sha256=RECEIPT)
    assert pin(facade, provider) == original
    assert facade.status().pins == 0
    assert not facade.authorize(original, provider.snapshot, provider.candle, decision_at_ms=60000)


def test_no_legacy_dispatch_fallback_and_native_control_types():
    facade, _, provider, _ = setup()
    provider.dispatch_feature_mutation = None
    provider.pin = lambda *args, **kwargs: pytest.fail("legacy direct side effect")
    with pytest.raises(FeatureMutationUnknown):
        pin(facade, provider)
    with pytest.raises(ValidationError):
        GenerationBinding("core", True, "actor", provider)
    with pytest.raises(ValidationError):
        facade.lookup(provider.binding, provider.candle, decision_at_ms=True)


def test_hot_capacity_preserves_32_release_slots_without_forgetting_unknowns():
    facade, broker, provider, _ = setup()
    originals = [pin(facade, provider, frame=digest({"old": n})) for n in range(32)]
    broker.prepare_error = TimeoutError()
    for index in range(96):
        with pytest.raises(FeatureMutationUnknown):
            pin(facade, provider, frame=digest({"new": index}))
    assert facade.status().hot_mutations == 96
    with pytest.raises(TradingBlocked):
        pin(facade, provider, frame=digest({"new": 96}))
    # New risk is held, while releases retain their own finite admission budget.
    facade.hold(SHA)
    for original in originals:
        with pytest.raises(FeatureMutationUnknown):
            facade.release(original, consumer_receipt_sha256=RECEIPT)
    status = facade.status()
    assert status.hot_mutations == 128 and status.unknown_mutations == 128
    assert status.pins == 32 and len(facade.pending_mutations()) == 128


def test_late_terminal_already_in_broker_is_not_mislabeled_as_recording_failure():
    facade, broker, provider, _ = setup()

    def finish_then_lose_waiter(ref):
        _, _, result_json = broker.calls[-1]
        broker.records[ref.sha256] = broker.ack(ref, "terminal", result_json=result_json)

    broker.on_finish = finish_then_lose_waiter
    broker.finish_error = TimeoutError()
    with pytest.raises(FeatureMutationUnknown) as caught:
        pin(facade, provider)
    assert not caught.value.recording_unknown
    assert facade.status().hot_mutations == 1 and facade.status().pins == 0
    ack = broker.records[caught.value.ref.sha256]
    assert facade.reconcile_mutation(caught.value.ref, ack)
    assert facade.status().pins == 1


def test_pin_evidence_capacity_includes_reserved_unknown_results():
    broker, provider = Broker(), Provider()
    facade = GenerationFeatureProvider(broker, lambda: 60001, pin_capacity=2)
    facade.bind_initial(GenerationBinding("core", 0, "actor0", provider))
    original = pin(facade, provider)
    broker.prepare_error = TimeoutError()
    with pytest.raises(FeatureMutationUnknown):
        pin(facade, provider, frame=SHA)
    with pytest.raises(TradingBlocked):
        pin(facade, provider, frame=RECEIPT)
    # Releases do not require another pin-evidence slot.
    with pytest.raises(FeatureMutationUnknown):
        facade.release(original, consumer_receipt_sha256=RECEIPT)
    assert facade.status().hot_mutations == 2 and facade.status().pins == 1


def test_cold_initial_hold_can_enable_only_after_explicit_current_manual_gate():
    broker, provider = Broker(), Provider()
    facade = GenerationFeatureProvider(broker, lambda: 60001)
    binding = GenerationBinding("core", 0, "actor0", provider)
    with pytest.raises(ValidationError):
        facade.bind_initial(binding, held=1)
    facade.bind_initial(binding, held=True)
    assert facade.status().held and facade.list_bindings() == ()
    assert facade.enable_current(0, "actor0", expected_control_revision=0)
    assert facade.list_bindings() == (provider.binding,)


def test_original_provider_positive_no_effect_rejection_gets_terminal_ack():
    facade, broker, provider, _ = setup()
    provider.dispatch_error = FeatureMutationRejected("original_pin_rejected")
    with pytest.raises(FeatureMutationRejected):
        pin(facade, provider)
    assert [row[0] for row in broker.calls] == ["prepare", "finish"]
    assert decoded(broker.calls[-1][2]) == {"not_dispatched": True}
    assert facade.status().hot_mutations == 0 and facade.status().pins == 0


def test_sequential_settlement_exceeds_active_capacity_without_lifetime_ceiling():
    broker, provider = Broker(), Provider()
    facade = GenerationFeatureProvider(broker, lambda: 60001, pin_capacity=2)
    facade.bind_initial(GenerationBinding("core", 0, "actor0", provider))
    first = pin(facade, provider)
    facade.release(first, consumer_receipt_sha256=RECEIPT)
    for index in range(140):
        current = pin(facade, provider, frame=digest({"cycle": index}))
        facade.release(current, consumer_receipt_sha256=RECEIPT)
    assert facade.status().pins == 0 and len(facade._released) == 128
    before = len([row for row in provider.calls if type(row) is tuple])
    # Evicted terminal replay checks the original provider's current cached
    # authority and cannot resurrect an already consumed parsed pin proof.
    assert pin(facade, provider) == first
    assert facade.status().pins == 0
    assert not facade.authorize(first, provider.snapshot, provider.candle, decision_at_ms=60000)
    assert len([row for row in provider.calls if type(row) is tuple]) == before
    facade.release(ExternalFeaturePin.parse(first.document()), consumer_receipt_sha256=RECEIPT)
    assert broker.calls[-1][0] == "lookup"
    assert len([row for row in provider.calls if type(row) is tuple]) == before
    forged = replace(first, created_at_ms=60001)
    with pytest.raises(TradingBlocked):
        facade.release(forged, consumer_receipt_sha256=RECEIPT)


def test_consumed_tombstone_cannot_evict_while_original_terminal_replay_is_in_flight(
    monkeypatch,
):
    broker, provider = Broker(), Provider()
    facade = GenerationFeatureProvider(broker, lambda: 60001, pin_capacity=2)
    facade.bind_initial(GenerationBinding("core", 0, "actor0", provider))
    first = pin(facade, provider)
    contains_pin = provider.contains_pin
    entered, proceed = threading.Event(), threading.Event()
    outcomes = []

    def delayed(pin):
        result = contains_pin(pin)
        if pin.pin_id == first.pin_id:
            entered.set()
            assert proceed.wait(3)
        return result

    monkeypatch.setattr(provider, "contains_pin", delayed)
    replay = threading.Thread(target=lambda: outcomes.append(pin(facade, provider)))
    replay.start()
    try:
        assert entered.wait(3)
        facade.release(first, consumer_receipt_sha256=RECEIPT)
        for index in range(140):
            current = pin(facade, provider, frame=digest({"concurrent_cycle": index}))
            facade.release(current, consumer_receipt_sha256=RECEIPT)
        assert (0, digest(first.document())) in facade._released
        assert len(facade._released) == 129  # One protected row plus bounded cache.
        proceed.set()
        replay.join(3)
        assert not replay.is_alive() and outcomes == [first]
        assert facade.status().pins == 0 and len(facade._released) == 128
    finally:
        proceed.set()
        replay.join(3)


def test_install_interlock_holds_new_risk_but_allows_original_pin_settlement():
    facade, _, provider, _ = setup()
    original = pin(facade, provider)
    before = facade.status()
    assert facade.fence_install("shutdown-check")
    fenced = facade.status()
    assert fenced.held and fenced.settlement_open and not fenced.closing
    assert fenced.shutdown_serial == before.shutdown_serial + 1
    assert fenced.control_revision == before.control_revision + 1
    assert facade.fence_install("shutdown-check")
    assert facade.status() == fenced
    assert not facade.fence_install("foreign-operation")
    assert not facade.enable_current(0, "actor0", expected_control_revision=fenced.control_revision)
    with pytest.raises(TradingBlocked):
        pin(facade, provider, frame=SHA)
    facade.release(original, consumer_receipt_sha256=RECEIPT)
    assert facade.status().pins == 0
    assert not facade.clear_install_fence("foreign-operation")
    assert facade.status().held
    assert facade.clear_install_fence("shutdown-check")
    cleared = facade.status()
    assert cleared.held and cleared.settlement_open
    assert cleared.shutdown_serial == fenced.shutdown_serial + 1
    assert cleared.control_revision == fenced.control_revision + 1
    assert not facade.clear_install_fence("shutdown-check")
    # Matching cancellation still requires a separate explicit manual action.
    assert facade.list_bindings() == ()
    assert facade.enable_current(0, "actor0", expected_control_revision=cleared.control_revision)


def test_install_interlock_invalidates_old_cas_before_host_cache_publication():
    facade, _, _, _ = setup()
    old = facade.hold(SHA)
    assert facade.close_settlement(old)
    assert facade.fence_install("core-close")
    assert not facade.install_cas(old, candidate())
    during = facade.hold(FRAME)
    assert not facade.close_settlement(during)
    assert not facade.install_cas(during, candidate())
    assert facade.clear_install_fence("core-close")
    assert not facade.install_cas(old, candidate())
    assert not facade.install_cas(during, candidate())
    current = facade.hold(RECEIPT)
    assert facade.close_settlement(current)
    assert facade.install_cas(current, candidate())


def test_terminal_shutdown_cannot_be_undone_by_matching_install_cancel():
    facade, _, _, _ = setup()
    assert facade.fence_install("core-close")
    facade.fence_shutdown()
    assert not facade.clear_install_fence("core-close")
    assert not facade.fence_install("core-close")
    assert facade.status().closing and not facade.status().settlement_open
    with pytest.raises(ValidationError):
        facade.fence_install(True)


def test_terminal_pin_remains_settleable_after_risk_expires_before_ack_return():
    facade, broker, provider, _ = setup()

    def pause_before_terminal_return(ref):
        if ref.kind == "pin":
            provider.risk_allowed = False

    broker.on_finish = pause_before_terminal_return
    original = pin(facade, provider)
    assert provider.contains_pin(original) and facade.status().pins == 1
    assert not facade.authorize(original, provider.snapshot, provider.candle, decision_at_ms=60000)
    with facade.entry_lease(
        original, provider.snapshot, provider.candle, decision_at_ms=60000
    ) as admitted:
        assert not admitted
    facade.hold(SHA)
    facade.release(original, consumer_receipt_sha256=RECEIPT)
    assert not provider.contains_pin(original) and provider.members == {}
    assert facade.status().pins == 0
    assert len([row for row in provider.calls if type(row) is tuple]) == 2


def test_original_membership_read_failure_retains_known_pin_result_for_reconciliation(
    monkeypatch,
):
    facade, broker, provider, _ = setup()
    contains_pin = provider.contains_pin

    def unavailable(pin):
        raise RuntimeError("cached membership read is unavailable")

    monkeypatch.setattr(provider, "contains_pin", unavailable)
    with pytest.raises(FeatureMutationUnknown) as caught:
        pin(facade, provider)
    assert caught.value.phase == "terminal" and not caught.value.recording_unknown
    assert facade.status().hot_mutations == 1 and len(provider.members) == 1
    monkeypatch.setattr(provider, "contains_pin", contains_pin)
    assert facade.reconcile_mutation(caught.value.ref, broker.records[caught.value.ref.sha256])
    assert facade.status().pins == 1
    original = next(iter(provider.members.values()))
    facade.release(original, consumer_receipt_sha256=RECEIPT)
    assert facade.status().pins == 0 and not provider.members


def test_prepared_terminal_replay_consumes_exact_original_ack_without_provider_dispatch():
    facade, broker, provider, _ = setup()
    original = pin(facade, provider)
    assert len(broker.consume_calls) == 1
    assert pin(facade, provider) == original
    assert len(broker.consume_calls) == 2
    ref, ack, witness = broker.consume_calls[-1]
    assert broker.records[ref.sha256] is ack
    assert witness.ref == ref and witness.ack is ack
    assert len([row for row in provider.calls if type(row) is tuple]) == 1
    assert facade.status().hot_mutations == 0


def test_lost_consumption_rpc_retains_original_applied_witness_until_same_ack_confirmed():
    facade, broker, provider, _ = setup()
    original = pin(facade, provider)

    def consume_then_lose_response(ref, ack, witness):
        assert facade.verify_applied_mutation(witness, ref, ack)
        broker.consumed[ref.sha256] = (ack, witness)

    broker.on_consume = consume_then_lose_response
    broker.consume_error = TimeoutError()
    with pytest.raises(FeatureMutationUnknown) as caught:
        pin(facade, provider)
    ref, ack, witness = broker.consume_calls[-1]
    assert caught.value.ref == ref and not caught.value.recording_unknown
    assert facade.status().hot_mutations == 1 and facade.status().unknown_mutations == 1
    assert facade.status().pins == 1
    assert facade.applied_mutation(ref, ack) is witness
    assert not facade.verify_applied_mutation(replace(witness), ref, ack)
    assert not facade.verify_applied_mutation(witness, ref, replace(ack))
    assert not facade.reconcile_mutation(ref, replace(ack))
    broker.consume_error = None
    assert facade.reconcile_mutation(ref, ack)
    assert facade.applied_mutation(ref, ack) is None
    assert facade.status().hot_mutations == 0
    assert broker.consume_calls[-1] == (ref, ack, witness)
    assert len([row for row in provider.calls if type(row) is tuple]) == 1
    facade.release(original, consumer_receipt_sha256=RECEIPT)
    assert not provider.members


def test_same_original_terminal_consumed_by_reconciler_before_first_waiter_returns():
    facade, broker, provider, _ = setup()

    def reconcile_before_return(ref, ack, witness):
        broker.on_consume = None
        assert facade.verify_applied_mutation(witness, ref, ack)
        assert facade.reconcile_mutation(ref, ack)

    broker.on_consume = reconcile_before_return
    original = pin(facade, provider)
    assert provider.contains_pin(original)
    assert facade.status().hot_mutations == 0
    assert len(broker.consume_calls) == 2
    assert len([row for row in provider.calls if type(row) is tuple]) == 1
